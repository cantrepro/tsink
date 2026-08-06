use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::Xxh64;

use super::tiering::SegmentLaneFamily;
use super::*;
use crate::engine::fs_utils::{
    collect_directory_entries_bounded, create_dir_all_and_sync_parents,
    remove_path_if_exists_and_sync_parent_budgeted, remove_path_if_exists_and_sync_parent_observed,
    write_file_atomically_and_sync_parent_budgeted, MAX_RECOVERY_NAMESPACE_ENTRIES,
};
use crate::engine::segment::IndexedSegment;
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::Label;

const REGISTRY_CATALOG_FILE_NAME: &str = "series_index.catalog.json";
const REGISTRY_CATALOG_VERSION: u32 = 2;
const REGISTRY_CATALOG_STORE_DIR_NAME: &str = "series_index.catalog.d";
const REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME: &str = "manifest.json";
const REGISTRY_CATALOG_STORE_VERSION: u32 = 1;
const REGISTRY_CATALOG_ENTRY_PREFIX: &str = "segment-";
const REGISTRY_CATALOG_ENTRY_SUFFIX: &str = ".json";
const REGISTRY_CATALOG_PENDING_DELTA_MAX_BYTES: usize = 16 * 1024 * 1024;
const REGISTRY_CATALOG_STORE_MANIFEST_MAX_BYTES: u64 = 17 * 1024 * 1024;
const REGISTRY_CATALOG_ENTRY_MAX_BYTES: u64 = 16 * 1024;
const REGISTRY_CATALOG_LEGACY_MAX_BYTES: u64 = 64 * 1024 * 1024;
const REGISTRY_CATALOG_MAX_LIVE_ENTRIES: usize = MAX_RECOVERY_NAMESPACE_ENTRIES - 2;

#[derive(Debug, Clone)]
pub(super) struct PersistedRegistryCatalogSource {
    pub(super) lane: SegmentLaneFamily,
    pub(super) root: PathBuf,
}

#[derive(Debug, Clone)]
pub(super) struct PersistedRegistryCatalogDelta {
    pub(super) added: Vec<PersistedRegistryCatalogSource>,
    pub(super) removed: Vec<PersistedRegistryCatalogEntryKey>,
}

impl PersistedRegistryCatalogDelta {
    pub(super) fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

#[derive(Debug, Clone)]
pub(super) enum PersistedRegistryCatalogUpdate {
    Complete(Vec<PersistedRegistryCatalogSource>),
    Delta(PersistedRegistryCatalogDelta),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogFile {
    version: u32,
    segments: Vec<PersistedRegistryCatalogEntry>,
    #[serde(default)]
    series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogEntry {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
    chunk_count: usize,
    point_count: usize,
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    wal_highwater_segment: u64,
    wal_highwater_frame: u64,
    chunks_len: u64,
    chunks_hash64: u64,
    chunk_index_len: u64,
    chunk_index_hash64: u64,
    series_len: u64,
    series_hash64: u64,
    postings_len: u64,
    postings_hash64: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct PersistedRegistryCatalogEntryKey {
    pub(super) lane: SegmentLaneFamily,
    pub(super) level: u8,
    pub(super) segment_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogStoreManifest {
    version: u32,
    complete: bool,
    entry_count: usize,
    #[serde(default)]
    series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
    #[serde(default)]
    pending_delta: Option<PersistedRegistryCatalogPendingDelta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogPendingDelta {
    added: Vec<PersistedRegistryCatalogEntry>,
    removed: Vec<PersistedRegistryCatalogEntryKey>,
    target_entry_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedRegistrySeriesFingerprint {
    series_count: usize,
    series_hash64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedRegistryCatalog {
    pub(super) series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
    pub(super) incremental_store: bool,
    pub(super) legacy_snapshot: bool,
}

pub(super) fn catalog_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_FILE_NAME))
}

pub(super) fn catalog_store_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_STORE_DIR_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_STORE_DIR_NAME))
}

pub(super) fn catalog_entry_key(
    lane: SegmentLaneFamily,
    manifest: &crate::engine::segment::SegmentManifest,
) -> PersistedRegistryCatalogEntryKey {
    PersistedRegistryCatalogEntryKey {
        lane,
        level: manifest.level,
        segment_id: manifest.segment_id,
    }
}

pub(super) fn catalog_entry_key_from_root(
    lane: SegmentLaneFamily,
    root: &Path,
) -> Result<PersistedRegistryCatalogEntryKey> {
    if root
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        != Some(std::ffi::OsStr::new("segments"))
    {
        return Err(TsinkError::DataCorruption(format!(
            "persisted segment root is outside the exact segment namespace: {}",
            root.display()
        )));
    }
    let level_name = root
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "persisted segment root has no UTF-8 level component: {}",
                root.display()
            ))
        })?;
    let level = level_name
        .strip_prefix('L')
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|level| *level <= 2 && level_name == format!("L{level}"))
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "persisted segment root has an invalid level component: {}",
                root.display()
            ))
        })?;
    let segment_name = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "persisted segment root has no UTF-8 segment component: {}",
                root.display()
            ))
        })?;
    let encoded = segment_name.strip_prefix("seg-").ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "persisted segment root has an invalid segment component: {}",
            root.display()
        ))
    })?;
    if encoded.len() != 16
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(TsinkError::DataCorruption(format!(
            "persisted segment root has an invalid segment id: {}",
            root.display()
        )));
    }
    let segment_id = u64::from_str_radix(encoded, 16).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "persisted segment root has an invalid segment id: {}",
            root.display()
        ))
    })?;
    Ok(PersistedRegistryCatalogEntryKey {
        lane,
        level,
        segment_id,
    })
}

pub(super) fn validate_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Option<ValidatedRegistryCatalog>> {
    let legacy = validate_legacy_registry_catalog(snapshot_path, sources)?;
    let legacy_present = match fs::symlink_metadata(catalog_path(snapshot_path)) {
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => return Err(err.into()),
    };
    if legacy_present && legacy.is_none() {
        return Ok(None);
    }
    let store = validate_registry_catalog_store(snapshot_path, sources)?;
    match (store, legacy) {
        (Some(mut store), Some(_)) => {
            store.legacy_snapshot = true;
            Ok(Some(store))
        }
        (Some(store), None) => Ok(Some(store)),
        (None, legacy) => Ok(legacy),
    }
}

fn validate_legacy_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Option<ValidatedRegistryCatalog>> {
    let path = catalog_path(snapshot_path);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if metadata.file_type().is_symlink() {
        return Ok(None);
    }
    if !metadata.is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "persisted registry catalog is not a regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > REGISTRY_CATALOG_LEGACY_MAX_BYTES {
        return Ok(None);
    }

    let catalog_bytes = std::fs::read(&path)?;
    let actual = match serde_json::from_slice::<PersistedRegistryCatalogFile>(&catalog_bytes) {
        Ok(actual) => actual,
        Err(_) => return Ok(None),
    };
    if actual.version != REGISTRY_CATALOG_VERSION {
        return Ok(None);
    }
    let expected_segments = build_catalog_entries(sources)?;
    if actual.segments != expected_segments {
        return Ok(None);
    }

    Ok(Some(ValidatedRegistryCatalog {
        series_fingerprint: actual.series_fingerprint,
        incremental_store: false,
        legacy_snapshot: true,
    }))
}

fn validate_registry_catalog_store(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Option<ValidatedRegistryCatalog>> {
    let store_path = catalog_store_path(snapshot_path);
    let metadata = match fs::symlink_metadata(&store_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Ok(None);
    }

    let entries = collect_directory_entries_bounded(
        &store_path,
        MAX_RECOVERY_NAMESPACE_ENTRIES,
        "persisted registry catalog store validation",
    )?;
    let mut manifest = None;
    let mut entry_count = 0usize;
    for directory_entry in entries {
        let file_type = directory_entry.file_type()?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Ok(None);
        }
        let file_name = directory_entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            return Ok(None);
        };
        if file_name == REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME {
            if directory_entry.metadata()?.len() > REGISTRY_CATALOG_STORE_MANIFEST_MAX_BYTES {
                return Ok(None);
            }
            let bytes = fs::read(directory_entry.path())?;
            manifest = serde_json::from_slice::<PersistedRegistryCatalogStoreManifest>(&bytes).ok();
        } else if parse_catalog_entry_file_name(file_name).is_some() {
            entry_count = entry_count.saturating_add(1);
        } else {
            return Ok(None);
        }
    }

    let Some(manifest) = manifest else {
        return Ok(None);
    };
    if manifest.version != REGISTRY_CATALOG_STORE_VERSION
        || !manifest.complete
        || manifest.pending_delta.is_some()
    {
        return Ok(None);
    }
    if manifest.entry_count != entry_count || entry_count != sources.len() {
        return Ok(None);
    }
    for source in sources {
        let expected = build_catalog_entry(source)?;
        let path = catalog_entry_path(&store_path, expected.key());
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Ok(None);
        }
        if metadata.len() > REGISTRY_CATALOG_ENTRY_MAX_BYTES {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        let actual = match serde_json::from_slice::<PersistedRegistryCatalogEntry>(&bytes) {
            Ok(actual) => actual,
            Err(_) => return Ok(None),
        };
        if actual != expected {
            return Ok(None);
        }
    }

    Ok(Some(ValidatedRegistryCatalog {
        series_fingerprint: manifest.series_fingerprint,
        incremental_store: true,
        legacy_snapshot: false,
    }))
}

pub(super) fn persist_registry_catalog_budgeted_with_kind(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    persist_registry_catalog_store_budgeted_with_kind(
        snapshot_path,
        sources,
        local_disk_budget,
        reservation_kind,
    )?;

    // Keep publishing the v2 file on complete checkpoints so older binaries can still open a
    // store written by this version. Bounded delta publication removes it after the per-segment
    // store is updated, preventing a stale aggregate from shadowing the exact incremental view.
    let path = catalog_path(snapshot_path);
    let bytes = serde_json::to_vec_pretty(&build_catalog(sources)?)?;
    if bytes.len() as u64 > REGISTRY_CATALOG_LEGACY_MAX_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "legacy persisted registry catalog snapshot",
            limit: REGISTRY_CATALOG_LEGACY_MAX_BYTES,
            required: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }
    write_file_atomically_and_sync_parent_budgeted(
        &path,
        &bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        reservation_kind,
    )
}

fn persist_registry_catalog_store_budgeted_with_kind(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    admit_catalog_namespace(sources.len(), sources.len())?;
    let store_path = catalog_store_path(snapshot_path);
    ensure_catalog_store_directory(&store_path)?;
    let incomplete = PersistedRegistryCatalogStoreManifest {
        version: REGISTRY_CATALOG_STORE_VERSION,
        complete: false,
        entry_count: 0,
        series_fingerprint: None,
        pending_delta: None,
    };
    persist_store_manifest(
        &store_path,
        &incomplete,
        local_disk_budget,
        reservation_kind,
    )?;

    let mut expected_file_names = BTreeSet::new();
    for source in sources {
        let entry = build_catalog_entry(source)?;
        let path = catalog_entry_path(&store_path, entry.key());
        if !expected_file_names.insert(
            path.file_name()
                .expect("catalog entry path has a file name")
                .to_os_string(),
        ) {
            return Err(TsinkError::DataCorruption(
                "persisted registry catalog sources contain duplicate segment identities"
                    .to_string(),
            ));
        }
        persist_store_entry(&path, &entry, local_disk_budget, reservation_kind)?;
    }

    let discovered = collect_directory_entries_bounded(
        &store_path,
        MAX_RECOVERY_NAMESPACE_ENTRIES,
        "persisted registry catalog store checkpoint",
    )?;
    for directory_entry in discovered {
        let file_name = directory_entry.file_name();
        if file_name == REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME
            || expected_file_names.contains(&file_name)
        {
            continue;
        }
        let Some(name) = file_name.to_str() else {
            return Err(TsinkError::DataCorruption(format!(
                "persisted registry catalog contains a non-UTF-8 entry: {}",
                directory_entry.path().display()
            )));
        };
        if parse_catalog_entry_file_name(name).is_none()
            && !is_catalog_atomic_temporary_file_name(name)
        {
            return Err(TsinkError::DataCorruption(format!(
                "persisted registry catalog contains an unrecognized entry: {}",
                directory_entry.path().display()
            )));
        }
        remove_path_if_exists_and_sync_parent_budgeted(
            &directory_entry.path(),
            local_disk_budget,
            crate::DiskCategory::Registry,
        )?;
    }

    let complete = PersistedRegistryCatalogStoreManifest {
        version: REGISTRY_CATALOG_STORE_VERSION,
        complete: true,
        entry_count: sources.len(),
        series_fingerprint: Some(build_series_fingerprint_from_sources(sources)?),
        pending_delta: None,
    };
    persist_store_manifest(&store_path, &complete, local_disk_budget, reservation_kind)
}

/// Starts an exact finite-writer reconciliation without enumerating or materializing the live
/// inventory. The incomplete marker makes every interrupted prefix fail closed during startup;
/// later cursor pages install the expected entries and remove stale ones before publishing the
/// terminal complete manifest.
pub(super) fn begin_bounded_registry_catalog_reconciliation(
    snapshot_path: &Path,
    expected_entries: usize,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    admit_catalog_namespace(expected_entries, expected_entries)?;
    let store_path = catalog_store_path(snapshot_path);
    ensure_catalog_store_directory(&store_path)?;
    persist_store_manifest(
        &store_path,
        &PersistedRegistryCatalogStoreManifest {
            version: REGISTRY_CATALOG_STORE_VERSION,
            complete: false,
            entry_count: 0,
            series_fingerprint: None,
            pending_delta: None,
        },
        local_disk_budget,
        crate::DiskReservationKind::Maintenance,
    )
}

pub(super) fn persist_bounded_registry_catalog_entry(
    snapshot_path: &Path,
    lane: SegmentLaneFamily,
    root: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let store_path = catalog_store_path(snapshot_path);
    let entry = build_catalog_entry_from_parts(lane, root)?;
    persist_store_entry(
        &catalog_entry_path(&store_path, entry.key()),
        &entry,
        local_disk_budget,
        crate::DiskReservationKind::Maintenance,
    )
}

/// Reconciles exactly one already-enumerated store entry. The caller owns pagination and passes a
/// predicate over the decoded canonical key, so this helper never scans or allocates the expected
/// inventory itself.
pub(super) fn reconcile_bounded_registry_catalog_store_entry(
    directory_entry: fs::DirEntry,
    expected: impl FnOnce(PersistedRegistryCatalogEntryKey) -> bool,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let file_type = directory_entry.file_type()?;
    if !file_type.is_file() || file_type.is_symlink() {
        return Err(TsinkError::DataCorruption(format!(
            "persisted registry catalog contains a non-regular entry: {}",
            directory_entry.path().display()
        )));
    }
    let file_name = directory_entry.file_name();
    let Some(file_name) = file_name.to_str() else {
        return Err(TsinkError::DataCorruption(format!(
            "persisted registry catalog contains a non-UTF-8 entry: {}",
            directory_entry.path().display()
        )));
    };
    if file_name == REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME {
        return Ok(());
    }
    let retain = parse_catalog_entry_file_name(file_name).is_some_and(expected);
    if retain {
        return Ok(());
    }
    if parse_catalog_entry_file_name(file_name).is_none()
        && !is_catalog_atomic_temporary_file_name(file_name)
    {
        return Err(TsinkError::DataCorruption(format!(
            "persisted registry catalog contains an unrecognized entry: {}",
            directory_entry.path().display()
        )));
    }
    remove_path_if_exists_and_sync_parent_budgeted(
        &directory_entry.path(),
        local_disk_budget,
        crate::DiskCategory::Registry,
    )
}

pub(super) fn complete_bounded_registry_catalog_reconciliation(
    snapshot_path: &Path,
    entry_count: usize,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let store_path = catalog_store_path(snapshot_path);
    persist_store_manifest(
        &store_path,
        &PersistedRegistryCatalogStoreManifest {
            version: REGISTRY_CATALOG_STORE_VERSION,
            complete: true,
            entry_count,
            // A finite reconciliation intentionally avoids the O(total series) aggregate
            // fingerprint. Startup treats its absence as a request to load per-segment metadata.
            series_fingerprint: None,
            pending_delta: None,
        },
        local_disk_budget,
        crate::DiskReservationKind::Maintenance,
    )
}

pub(super) fn retire_legacy_registry_catalog_after_bounded_reconciliation(
    snapshot_path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    remove_path_if_exists_and_sync_parent_budgeted(
        &catalog_path(snapshot_path),
        local_disk_budget,
        crate::DiskCategory::Registry,
    )
}

pub(super) fn persist_registry_catalog_delta_budgeted_with_kind(
    snapshot_path: &Path,
    delta: &PersistedRegistryCatalogDelta,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    if delta.is_empty() {
        return Ok(());
    }

    let store_path = catalog_store_path(snapshot_path);
    let mut manifest = match load_store_manifest(&store_path)? {
        Some(manifest) if manifest.version == REGISTRY_CATALOG_STORE_VERSION => manifest,
        Some(_) | None => {
            // A legacy-only catalog remains readable, but converting every legacy entry inside a
            // bounded page would reintroduce the complete-inventory spike this path avoids.
            // Publish an explicitly incomplete marker and retire the stale legacy aggregate. The
            // next complete/startup checkpoint can rebuild the native store without ever claiming
            // that this partial overlay is authoritative.
            ensure_catalog_store_directory(&store_path)?;
            let incomplete = PersistedRegistryCatalogStoreManifest {
                version: REGISTRY_CATALOG_STORE_VERSION,
                complete: false,
                entry_count: 0,
                series_fingerprint: None,
                pending_delta: None,
            };
            persist_store_manifest(
                &store_path,
                &incomplete,
                local_disk_budget,
                reservation_kind,
            )?;
            remove_path_if_exists_and_sync_parent_budgeted(
                &catalog_path(snapshot_path),
                local_disk_budget,
                crate::DiskCategory::Registry,
            )?;
            return Ok(());
        }
    };

    let mut added_entries = BTreeMap::new();
    for source in &delta.added {
        let entry = build_catalog_entry(source)?;
        added_entries.insert(entry.key(), entry);
    }
    let removed_keys = delta.removed.iter().copied().collect::<BTreeSet<_>>();
    let added_keys = added_entries.keys().copied().collect::<BTreeSet<_>>();
    let desired_added = added_entries.values().cloned().collect::<Vec<_>>();
    let desired_removed = removed_keys
        .difference(&added_keys)
        .copied()
        .collect::<Vec<_>>();

    let (pending, publish_intent) = if let Some(pending) = manifest.pending_delta.clone() {
        if pending.added != desired_added || pending.removed != desired_removed {
            return Err(TsinkError::DataCorruption(
                "persisted registry catalog has a different incomplete root delta".to_string(),
            ));
        }
        (pending, false)
    } else {
        if !manifest.complete {
            // A legacy migration marker or a torn complete rebuild cannot be made exact from one
            // bounded root delta. Keep it explicitly incomplete for the next complete checkpoint.
            remove_path_if_exists_and_sync_parent_budgeted(
                &catalog_path(snapshot_path),
                local_disk_budget,
                crate::DiskCategory::Registry,
            )?;
            return Ok(());
        }

        let newly_added = count_missing_catalog_entries(&store_path, added_entries.keys())?;
        let removed_count =
            count_existing_catalog_entries(&store_path, desired_removed.iter().copied())?;
        let target_entry_count = manifest
            .entry_count
            .saturating_add(newly_added)
            .saturating_sub(removed_count);
        admit_catalog_namespace(manifest.entry_count, target_entry_count)?;

        let pending = PersistedRegistryCatalogPendingDelta {
            added: desired_added,
            removed: desired_removed,
            target_entry_count,
        };
        let pending_bytes = serde_json::to_vec(&pending)?.len();
        if pending_bytes > REGISTRY_CATALOG_PENDING_DELTA_MAX_BYTES {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: "persisted registry catalog root delta",
                limit: REGISTRY_CATALOG_PENDING_DELTA_MAX_BYTES as u64,
                required: u64::try_from(pending_bytes).unwrap_or(u64::MAX),
            });
        }

        // Publish the bounded intent and invalidate the aggregate fingerprint before touching an
        // entry file. Validation rejects `complete = false`; retry replays this exact intent
        // idempotently and publishes its recorded final count.
        manifest.complete = false;
        manifest.series_fingerprint = None;
        manifest.pending_delta = Some(pending.clone());
        (pending, true)
    };

    persist_planned_registry_catalog_delta(
        snapshot_path,
        &store_path,
        &mut manifest,
        &pending,
        publish_intent,
        local_disk_budget,
        reservation_kind,
    )
}

fn persist_planned_registry_catalog_delta(
    snapshot_path: &Path,
    store_path: &Path,
    manifest: &mut PersistedRegistryCatalogStoreManifest,
    pending: &PersistedRegistryCatalogPendingDelta,
    publish_intent: bool,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    let apply = |manifest: &mut PersistedRegistryCatalogStoreManifest,
                 local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>| {
        apply_planned_registry_catalog_delta(
            snapshot_path,
            store_path,
            manifest,
            pending,
            publish_intent,
            local_disk_budget,
            reservation_kind,
        )
    };
    let Some(budget) = local_disk_budget else {
        return apply(manifest, None);
    };
    let manifest_path = store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME);
    let legacy_path = catalog_path(snapshot_path);
    if !budget.governs_entry(&manifest_path)? || !budget.governs_entry(&legacy_path)? {
        return apply(manifest, Some(budget));
    }

    let peak_bytes =
        planned_registry_catalog_delta_publication_peak(manifest, pending, publish_intent)?;
    budget.with_strict_reconciled_reservation(
        crate::DiskCategory::Registry,
        peak_bytes,
        reservation_kind,
        || apply(manifest, None),
    )
}

fn apply_planned_registry_catalog_delta(
    snapshot_path: &Path,
    store_path: &Path,
    manifest: &mut PersistedRegistryCatalogStoreManifest,
    pending: &PersistedRegistryCatalogPendingDelta,
    publish_intent: bool,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    if publish_intent {
        persist_store_manifest(store_path, manifest, local_disk_budget, reservation_kind)?;
    }
    remove_registry_catalog_entries_budgeted(store_path, &pending.removed, local_disk_budget)?;
    for entry in &pending.added {
        persist_store_entry(
            &catalog_entry_path(store_path, entry.key()),
            entry,
            local_disk_budget,
            reservation_kind,
        )?;
    }

    manifest.complete = true;
    manifest.entry_count = pending.target_entry_count;
    manifest.series_fingerprint = None;
    manifest.pending_delta = None;
    persist_store_manifest(store_path, manifest, local_disk_budget, reservation_kind)?;

    remove_path_if_exists_and_sync_parent_budgeted(
        &catalog_path(snapshot_path),
        local_disk_budget,
        crate::DiskCategory::Registry,
    )
}

fn planned_registry_catalog_delta_publication_peak(
    manifest: &PersistedRegistryCatalogStoreManifest,
    pending: &PersistedRegistryCatalogPendingDelta,
    publish_intent: bool,
) -> Result<u64> {
    let mut peak_bytes = 0u64;
    if publish_intent {
        peak_bytes = peak_bytes
            .checked_add(u64::try_from(encode_store_manifest(manifest)?.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                TsinkError::Other("registry catalog delta publication peak overflowed".to_string())
            })?;
    }
    for entry in &pending.added {
        peak_bytes = peak_bytes
            .checked_add(u64::try_from(encode_store_entry(entry)?.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                TsinkError::Other("registry catalog delta publication peak overflowed".to_string())
            })?;
    }
    let mut complete = manifest.clone();
    complete.complete = true;
    complete.entry_count = pending.target_entry_count;
    complete.series_fingerprint = None;
    complete.pending_delta = None;
    peak_bytes
        .checked_add(u64::try_from(encode_store_manifest(&complete)?.len()).unwrap_or(u64::MAX))
        .ok_or_else(|| {
            TsinkError::Other("registry catalog delta publication peak overflowed".to_string())
        })
}

fn remove_registry_catalog_entries_budgeted(
    store_path: &Path,
    keys: &[PersistedRegistryCatalogEntryKey],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let mut reservation = None;
    let mut governed_deletion_or_ambiguous_error = false;
    let operation_result = (|| -> Result<()> {
        for key in keys {
            let path = catalog_entry_path(store_path, *key);
            let governed = match local_disk_budget {
                Some(budget) => budget.governs_entry(&path)?,
                None => false,
            };
            if governed && reservation.is_none() {
                reservation = Some(
                    local_disk_budget
                        .expect("governed registry catalog cleanup requires a disk budget")
                        .reserve(
                            crate::DiskCategory::Registry,
                            0,
                            crate::DiskReservationKind::Recovery,
                        )?,
                );
            }
            match remove_path_if_exists_and_sync_parent_observed(&path) {
                Ok(removed) => {
                    governed_deletion_or_ambiguous_error |= governed && removed;
                }
                Err(err) => {
                    governed_deletion_or_ambiguous_error |= governed;
                    return Err(err);
                }
            }
        }
        Ok(())
    })();

    let settlement_result = reservation.map_or(Ok(()), |reservation| reservation.commit(0, 0));
    if settlement_result.is_err() {
        governed_deletion_or_ambiguous_error = true;
    }
    let reconciliation_result = if governed_deletion_or_ambiguous_error {
        local_disk_budget
            .expect("governed registry catalog cleanup requires a disk budget")
            .reconcile_when_idle()
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
            "registry catalog entry cleanup in {} failed: {}",
            store_path.display(),
            errors.join("; ")
        )))
    }
}

fn ensure_catalog_store_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if !crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                && metadata.file_type().is_dir() =>
        {
            Ok(())
        }
        Ok(_) => Err(TsinkError::DataCorruption(format!(
            "persisted registry catalog store is link-like or not a directory: {}",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            create_dir_all_and_sync_parents(path)
        }
        Err(err) => Err(err.into()),
    }
}

fn admit_catalog_namespace(current: usize, target: usize) -> Result<()> {
    let required = current.max(target);
    if required > REGISTRY_CATALOG_MAX_LIVE_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "persisted registry catalog store",
            limit: REGISTRY_CATALOG_MAX_LIVE_ENTRIES,
            required,
        });
    }
    Ok(())
}

#[cfg(test)]
fn load_complete_store_manifest(
    store_path: &Path,
) -> Result<Option<PersistedRegistryCatalogStoreManifest>> {
    let Some(manifest) = load_store_manifest(store_path)? else {
        return Ok(None);
    };
    if manifest.version != REGISTRY_CATALOG_STORE_VERSION
        || !manifest.complete
        || manifest.pending_delta.is_some()
    {
        return Ok(None);
    }
    Ok(Some(manifest))
}

fn load_store_manifest(store_path: &Path) -> Result<Option<PersistedRegistryCatalogStoreManifest>> {
    let path = store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(None);
    }
    if metadata.len() > REGISTRY_CATALOG_STORE_MANIFEST_MAX_BYTES {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let manifest = match serde_json::from_slice::<PersistedRegistryCatalogStoreManifest>(&bytes) {
        Ok(manifest) => manifest,
        Err(_) => return Ok(None),
    };
    Ok(Some(manifest))
}

fn catalog_entry_exists(store_path: &Path, key: PersistedRegistryCatalogEntryKey) -> Result<bool> {
    let path = catalog_entry_path(store_path, key);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(TsinkError::DataCorruption(format!(
            "persisted registry catalog entry is not a regular file: {}",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

fn count_missing_catalog_entries<'a>(
    store_path: &Path,
    keys: impl IntoIterator<Item = &'a PersistedRegistryCatalogEntryKey>,
) -> Result<usize> {
    let mut count = 0usize;
    for key in keys {
        if !catalog_entry_exists(store_path, *key)? {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

fn count_existing_catalog_entries(
    store_path: &Path,
    keys: impl IntoIterator<Item = PersistedRegistryCatalogEntryKey>,
) -> Result<usize> {
    let mut count = 0usize;
    for key in keys {
        if catalog_entry_exists(store_path, key)? {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

fn persist_store_manifest(
    store_path: &Path,
    manifest: &PersistedRegistryCatalogStoreManifest,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    let bytes = encode_store_manifest(manifest)?;
    write_file_atomically_and_sync_parent_budgeted(
        &store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME),
        &bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        reservation_kind,
    )
}

fn encode_store_manifest(manifest: &PersistedRegistryCatalogStoreManifest) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(manifest)?;
    if bytes.len() as u64 > REGISTRY_CATALOG_STORE_MANIFEST_MAX_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "persisted registry catalog manifest",
            limit: REGISTRY_CATALOG_STORE_MANIFEST_MAX_BYTES,
            required: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }
    Ok(bytes)
}

fn persist_store_entry(
    path: &Path,
    entry: &PersistedRegistryCatalogEntry,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    let bytes = encode_store_entry(entry)?;
    write_file_atomically_and_sync_parent_budgeted(
        path,
        &bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        reservation_kind,
    )
}

fn encode_store_entry(entry: &PersistedRegistryCatalogEntry) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(entry)?;
    if bytes.len() as u64 > REGISTRY_CATALOG_ENTRY_MAX_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "persisted registry catalog segment entry",
            limit: REGISTRY_CATALOG_ENTRY_MAX_BYTES,
            required: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }
    Ok(bytes)
}

fn catalog_entry_path(store_path: &Path, key: PersistedRegistryCatalogEntryKey) -> PathBuf {
    let lane = match key.lane {
        SegmentLaneFamily::Numeric => "numeric",
        SegmentLaneFamily::Blob => "blob",
    };
    store_path.join(format!(
        "{REGISTRY_CATALOG_ENTRY_PREFIX}{lane}-{:02x}-{:016x}{REGISTRY_CATALOG_ENTRY_SUFFIX}",
        key.level, key.segment_id
    ))
}

fn parse_catalog_entry_file_name(name: &str) -> Option<PersistedRegistryCatalogEntryKey> {
    let encoded = name
        .strip_prefix(REGISTRY_CATALOG_ENTRY_PREFIX)?
        .strip_suffix(REGISTRY_CATALOG_ENTRY_SUFFIX)?;
    let (lane, remainder) = encoded.split_once('-')?;
    let lane = match lane {
        "numeric" => SegmentLaneFamily::Numeric,
        "blob" => SegmentLaneFamily::Blob,
        _ => return None,
    };
    let (level, segment_id) = remainder.split_once('-')?;
    if level.len() != 2
        || segment_id.len() != 16
        || !level.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !segment_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(PersistedRegistryCatalogEntryKey {
        lane,
        level: u8::from_str_radix(level, 16).ok()?,
        segment_id: u64::from_str_radix(segment_id, 16).ok()?,
    })
}

fn is_catalog_atomic_temporary_file_name(name: &str) -> bool {
    name.starts_with(".manifest.json.tmp-")
        || (name.starts_with(".segment-") && name.contains(".json.tmp-"))
}

pub(super) fn inventory_sources(
    inventory: &super::tiering::SegmentInventory,
) -> Vec<PersistedRegistryCatalogSource> {
    inventory
        .entries()
        .iter()
        .map(|entry| PersistedRegistryCatalogSource {
            lane: entry.lane,
            root: entry.root.clone(),
        })
        .collect()
}

pub(super) fn validate_registry_snapshot(
    registry: &SeriesRegistry,
    indexed_segments: &[IndexedSegment],
    expected: &PersistedRegistrySeriesFingerprint,
) -> Result<()> {
    let series_ids = indexed_segment_series_ids(indexed_segments);
    let actual = fingerprint_registry_series(registry, &series_ids)?;
    if actual == *expected {
        return Ok(());
    }

    Err(TsinkError::DataCorruption(
        "persisted registry snapshot conflicts with persisted segment series metadata".to_string(),
    ))
}

fn build_catalog(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<PersistedRegistryCatalogFile> {
    Ok(PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: build_catalog_entries(sources)?,
        series_fingerprint: Some(build_series_fingerprint_from_sources(sources)?),
    })
}

fn build_catalog_entries(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Vec<PersistedRegistryCatalogEntry>> {
    let mut segments = sources
        .iter()
        .map(build_catalog_entry)
        .collect::<Result<Vec<_>>>()?;
    segments.sort_by_key(|entry| (entry.lane, entry.level, entry.segment_id));
    Ok(segments)
}

fn build_catalog_entry(
    source: &PersistedRegistryCatalogSource,
) -> Result<PersistedRegistryCatalogEntry> {
    build_catalog_entry_from_parts(source.lane, &source.root)
}

fn build_catalog_entry_from_parts(
    lane: SegmentLaneFamily,
    root: &Path,
) -> Result<PersistedRegistryCatalogEntry> {
    let fingerprint = crate::engine::segment::read_segment_manifest_fingerprint(root)?;
    let [chunks, chunk_index, series, postings] = fingerprint.files;
    Ok(PersistedRegistryCatalogEntry {
        lane,
        level: fingerprint.manifest.level,
        segment_id: fingerprint.manifest.segment_id,
        chunk_count: fingerprint.manifest.chunk_count,
        point_count: fingerprint.manifest.point_count,
        series_count: fingerprint.manifest.series_count,
        min_ts: fingerprint.manifest.min_ts,
        max_ts: fingerprint.manifest.max_ts,
        wal_highwater_segment: fingerprint.manifest.wal_highwater.segment,
        wal_highwater_frame: fingerprint.manifest.wal_highwater.frame,
        chunks_len: chunks.file_len,
        chunks_hash64: chunks.hash64,
        chunk_index_len: chunk_index.file_len,
        chunk_index_hash64: chunk_index.hash64,
        series_len: series.file_len,
        series_hash64: series.hash64,
        postings_len: postings.file_len,
        postings_hash64: postings.hash64,
    })
}

impl PersistedRegistryCatalogEntry {
    fn key(&self) -> PersistedRegistryCatalogEntryKey {
        PersistedRegistryCatalogEntryKey {
            lane: self.lane,
            level: self.level,
            segment_id: self.segment_id,
        }
    }
}

fn build_series_fingerprint_from_sources(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series_by_id = BTreeMap::<SeriesId, crate::engine::segment::PersistedSeries>::new();
    let mut series_id_by_key = BTreeMap::<(String, Vec<Label>), SeriesId>::new();

    for source in sources {
        for series in crate::engine::segment::load_segment_series_metadata(&source.root)? {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels => {}
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts across persisted segment metadata",
                        series.series_id
                    )));
                }
                None => {
                    let key = (series.metric.clone(), series.labels.clone());
                    if let Some(existing_id) = series_id_by_key.get(&key) {
                        if *existing_id != series.series_id {
                            return Err(TsinkError::DataCorruption(format!(
                                "series key already bound to id {}, persisted segment metadata tried to bind {}",
                                existing_id, series.series_id
                            )));
                        }
                    } else {
                        series_id_by_key.insert(key, series.series_id);
                    }
                    series_by_id.insert(series.series_id, series);
                }
            }
        }
    }

    Ok(fingerprint_series(series_by_id.values()))
}

fn indexed_segment_series_ids(indexed_segments: &[IndexedSegment]) -> Vec<SeriesId> {
    let mut series_ids = BTreeSet::new();
    for segment in indexed_segments {
        for entry in &segment.chunk_index.entries {
            series_ids.insert(entry.series_id);
        }
    }
    series_ids.into_iter().collect()
}

fn fingerprint_registry_series(
    registry: &SeriesRegistry,
    series_ids: &[SeriesId],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series = Vec::with_capacity(series_ids.len());
    for series_id in series_ids {
        let Some(series_key) = registry.decode_series_key(*series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "persisted registry snapshot is missing series id {} required by persisted segments",
                series_id
            )));
        };
        series.push(crate::engine::segment::PersistedSeries {
            series_id: *series_id,
            metric: series_key.metric,
            labels: series_key.labels,
            value_family: None,
        });
    }
    Ok(fingerprint_series(series.iter()))
}

fn fingerprint_series<'a>(
    series: impl IntoIterator<Item = &'a crate::engine::segment::PersistedSeries>,
) -> PersistedRegistrySeriesFingerprint {
    let mut count = 0usize;
    let mut hasher = Xxh64::new(0);
    for series in series {
        count = count.saturating_add(1);
        hasher.update(&series.series_id.to_le_bytes());
        update_len_prefixed_bytes(&mut hasher, series.metric.as_bytes());
        hasher.update(&(series.labels.len() as u64).to_le_bytes());
        for label in &series.labels {
            update_len_prefixed_bytes(&mut hasher, label.name.as_bytes());
            update_len_prefixed_bytes(&mut hasher, label.value.as_bytes());
        }
    }
    PersistedRegistrySeriesFingerprint {
        series_count: count,
        series_hash64: hasher.digest(),
    }
}

fn update_len_prefixed_bytes(hasher: &mut Xxh64, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn fake_entry(segment_id: u64) -> PersistedRegistryCatalogEntry {
        PersistedRegistryCatalogEntry {
            lane: SegmentLaneFamily::Numeric,
            level: 0,
            segment_id,
            chunk_count: 1,
            point_count: 1,
            series_count: 1,
            min_ts: Some(segment_id as i64),
            max_ts: Some(segment_id as i64),
            wal_highwater_segment: 0,
            wal_highwater_frame: 0,
            chunks_len: 1,
            chunks_hash64: segment_id,
            chunk_index_len: 1,
            chunk_index_hash64: segment_id,
            series_len: 1,
            series_hash64: segment_id,
            postings_len: 1,
            postings_hash64: segment_id,
        }
    }

    fn seed_native_store(snapshot_path: &Path, segment_count: u64) {
        let store_path = catalog_store_path(snapshot_path);
        fs::create_dir_all(&store_path).unwrap();
        for segment_id in 1..=segment_count {
            let entry = fake_entry(segment_id);
            fs::write(
                catalog_entry_path(&store_path, entry.key()),
                serde_json::to_vec(&entry).unwrap(),
            )
            .unwrap();
        }
        fs::write(
            store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME),
            serde_json::to_vec(&PersistedRegistryCatalogStoreManifest {
                version: REGISTRY_CATALOG_STORE_VERSION,
                complete: true,
                entry_count: segment_count as usize,
                series_fingerprint: Some(PersistedRegistrySeriesFingerprint {
                    series_count: 1,
                    series_hash64: 1,
                }),
                pending_delta: None,
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn one_delta_removal_from_many_segments_touches_only_its_entry_and_manifest() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 4_096);
        let store_path = catalog_store_path(&snapshot_path);
        let untouched_path = catalog_entry_path(&store_path, fake_entry(4_096).key());
        let untouched_before = fs::read(&untouched_path).unwrap();

        persist_registry_catalog_delta_budgeted_with_kind(
            &snapshot_path,
            &PersistedRegistryCatalogDelta {
                added: Vec::new(),
                removed: vec![fake_entry(2_048).key()],
            },
            None,
            crate::DiskReservationKind::Maintenance,
        )
        .unwrap();

        assert!(!catalog_entry_path(&store_path, fake_entry(2_048).key()).exists());
        assert_eq!(fs::read(&untouched_path).unwrap(), untouched_before);
        let manifest = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("the native store should remain complete");
        assert_eq!(manifest.entry_count, 4_095);
        assert!(manifest.series_fingerprint.is_none());
        assert_eq!(fs::read_dir(&store_path).unwrap().count(), 4_096);
    }

    #[test]
    fn pending_delta_never_validates_as_a_complete_catalog() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        let store_path = catalog_store_path(&snapshot_path);
        fs::create_dir_all(&store_path).unwrap();
        let pending = PersistedRegistryCatalogPendingDelta {
            added: Vec::new(),
            removed: vec![fake_entry(1).key()],
            target_entry_count: 0,
        };
        fs::write(
            store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME),
            serde_json::to_vec(&PersistedRegistryCatalogStoreManifest {
                version: REGISTRY_CATALOG_STORE_VERSION,
                complete: false,
                entry_count: 0,
                series_fingerprint: None,
                pending_delta: Some(pending),
            })
            .unwrap(),
        )
        .unwrap();

        assert!(validate_registry_catalog_store(&snapshot_path, &[])
            .unwrap()
            .is_none());
    }

    #[test]
    fn interrupted_delta_replays_its_intent_idempotently() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 3);
        let store_path = catalog_store_path(&snapshot_path);
        let removed_key = fake_entry(2).key();
        let mut manifest = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("seeded manifest should load");
        manifest.complete = false;
        manifest.series_fingerprint = None;
        manifest.pending_delta = Some(PersistedRegistryCatalogPendingDelta {
            added: Vec::new(),
            removed: vec![removed_key],
            target_entry_count: 2,
        });
        fs::write(
            store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        fs::remove_file(catalog_entry_path(&store_path, removed_key)).unwrap();

        persist_registry_catalog_delta_budgeted_with_kind(
            &snapshot_path,
            &PersistedRegistryCatalogDelta {
                added: Vec::new(),
                removed: vec![removed_key],
            },
            None,
            crate::DiskReservationKind::Maintenance,
        )
        .unwrap();

        let completed = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("retry should finish the pending delta");
        assert_eq!(completed.entry_count, 2);
        assert!(completed.pending_delta.is_none());
        assert!(completed.series_fingerprint.is_none());
        assert_eq!(fs::read_dir(&store_path).unwrap().count(), 3);
    }

    #[test]
    fn delta_removal_reconciles_managed_disk_accounting() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 8);
        let budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();

        persist_registry_catalog_delta_budgeted_with_kind(
            &snapshot_path,
            &PersistedRegistryCatalogDelta {
                added: Vec::new(),
                removed: vec![fake_entry(4).key()],
            },
            Some(&budget),
            crate::DiskReservationKind::Maintenance,
        )
        .unwrap();

        assert_eq!(
            budget.snapshot().accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp.path()).unwrap()
        );
        assert_eq!(budget.snapshot().active_reservations, 0);
    }

    #[test]
    fn planned_delta_batches_overwrites_and_retirement_into_one_reconciliation() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 8);
        let store_path = catalog_store_path(&snapshot_path);
        let legacy_path = catalog_path(&snapshot_path);
        fs::write(&legacy_path, b"legacy catalog").unwrap();
        let unknown = store_path.join("host-owned");
        fs::write(&unknown, b"external").unwrap();
        let mut first_added = fake_entry(2);
        first_added.point_count = 20;
        let mut second_added = fake_entry(4);
        second_added.point_count = 40;
        let pending = PersistedRegistryCatalogPendingDelta {
            added: vec![first_added.clone(), second_added.clone()],
            removed: vec![fake_entry(6).key()],
            target_entry_count: 7,
        };
        let mut manifest = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("seeded manifest should load");
        manifest.complete = false;
        manifest.series_fingerprint = None;
        manifest.pending_delta = Some(pending.clone());
        let budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let before = budget.snapshot();
        assert_eq!(before.reconciliations_total, 1);

        persist_planned_registry_catalog_delta(
            &snapshot_path,
            &store_path,
            &mut manifest,
            &pending,
            true,
            Some(&budget),
            crate::DiskReservationKind::Maintenance,
        )
        .unwrap();

        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            snapshot.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp.path()).unwrap()
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
        assert_eq!(
            serde_json::from_slice::<PersistedRegistryCatalogEntry>(
                &fs::read(catalog_entry_path(&store_path, first_added.key())).unwrap()
            )
            .unwrap(),
            first_added
        );
        assert_eq!(
            serde_json::from_slice::<PersistedRegistryCatalogEntry>(
                &fs::read(catalog_entry_path(&store_path, second_added.key())).unwrap()
            )
            .unwrap(),
            second_added
        );
        assert!(!catalog_entry_path(&store_path, fake_entry(6).key()).exists());
        assert!(!legacy_path.exists());
        assert!(unknown.is_file());
        let completed = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("the completed manifest should load");
        assert_eq!(completed.entry_count, 7);
        assert!(completed.pending_delta.is_none());
    }

    #[test]
    fn planned_delta_intent_sync_failure_reconciles_and_retries_idempotently() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 4);
        let store_path = catalog_store_path(&snapshot_path);
        let legacy_path = catalog_path(&snapshot_path);
        fs::write(&legacy_path, b"legacy catalog").unwrap();
        let mut added = fake_entry(2);
        added.point_count = 200;
        let removed = fake_entry(3).key();
        let pending = PersistedRegistryCatalogPendingDelta {
            added: vec![added.clone()],
            removed: vec![removed],
            target_entry_count: 3,
        };
        let mut manifest = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("seeded manifest should load");
        manifest.complete = false;
        manifest.series_fingerprint = None;
        manifest.pending_delta = Some(pending.clone());
        let budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let before = budget.snapshot();
        let sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            store_path.clone(),
            "injected registry catalog intent sync failure",
        );

        let err = persist_planned_registry_catalog_delta(
            &snapshot_path,
            &store_path,
            &mut manifest,
            &pending,
            true,
            Some(&budget),
            crate::DiskReservationKind::Maintenance,
        )
        .expect_err("the committed incomplete intent must retain its synchronization error");
        drop(sync_failure);

        assert!(matches!(
            err,
            TsinkError::Other(ref message)
                if message == "injected registry catalog intent sync failure"
        ));
        let after_fault = budget.snapshot();
        assert_eq!(
            after_fault.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(after_fault.active_reservations, 0);
        let mut recovered = load_store_manifest(&store_path)
            .unwrap()
            .expect("the durable incomplete intent should load");
        assert!(!recovered.complete);
        assert_eq!(recovered.pending_delta.as_ref(), Some(&pending));
        assert_eq!(
            serde_json::from_slice::<PersistedRegistryCatalogEntry>(
                &fs::read(catalog_entry_path(&store_path, added.key())).unwrap()
            )
            .unwrap(),
            fake_entry(2)
        );
        assert!(catalog_entry_path(&store_path, removed).is_file());
        assert!(legacy_path.is_file());

        persist_planned_registry_catalog_delta(
            &snapshot_path,
            &store_path,
            &mut recovered,
            &pending,
            false,
            Some(&budget),
            crate::DiskReservationKind::Maintenance,
        )
        .unwrap();

        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.reconciliations_total,
            before.reconciliations_total + 2
        );
        assert_eq!(
            snapshot.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp.path()).unwrap()
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
        assert_eq!(
            serde_json::from_slice::<PersistedRegistryCatalogEntry>(
                &fs::read(catalog_entry_path(&store_path, added.key())).unwrap()
            )
            .unwrap(),
            added
        );
        assert!(!catalog_entry_path(&store_path, removed).exists());
        assert!(!legacy_path.exists());
        let completed = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("the retry should publish the complete manifest");
        assert_eq!(completed.entry_count, 3);
        assert!(completed.pending_delta.is_none());
    }

    #[test]
    fn catalog_entry_removal_batches_managed_entries_into_one_reconciliation() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 8);
        let store_path = catalog_store_path(&snapshot_path);
        let budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let before = budget.snapshot();
        assert_eq!(before.reconciliations_total, 1);

        remove_registry_catalog_entries_budgeted(
            &store_path,
            &[
                fake_entry(2).key(),
                fake_entry(4).key(),
                fake_entry(6).key(),
            ],
            Some(&budget),
        )
        .unwrap();

        assert!(!catalog_entry_path(&store_path, fake_entry(2).key()).exists());
        assert!(!catalog_entry_path(&store_path, fake_entry(4).key()).exists());
        assert!(!catalog_entry_path(&store_path, fake_entry(6).key()).exists());
        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            snapshot.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp.path()).unwrap()
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    }

    #[test]
    fn delta_removal_reconciles_once_after_post_unlink_sync_failure() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        seed_native_store(&snapshot_path, 4);
        let store_path = catalog_store_path(&snapshot_path);
        let first_removed = fake_entry(2).key();
        let later_removal = fake_entry(3).key();
        let mut manifest = load_complete_store_manifest(&store_path)
            .unwrap()
            .expect("seeded manifest should load");
        manifest.complete = false;
        manifest.series_fingerprint = None;
        manifest.pending_delta = Some(PersistedRegistryCatalogPendingDelta {
            added: Vec::new(),
            removed: vec![first_removed, later_removal],
            target_entry_count: 2,
        });
        fs::write(
            store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let before = budget.snapshot();
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            store_path.clone(),
            "injected registry catalog cleanup sync failure",
        );

        let err = persist_registry_catalog_delta_budgeted_with_kind(
            &snapshot_path,
            &PersistedRegistryCatalogDelta {
                added: Vec::new(),
                removed: vec![first_removed, later_removal],
            },
            Some(&budget),
            crate::DiskReservationKind::Maintenance,
        )
        .expect_err("the committed unlink must retain its synchronization error");

        assert!(matches!(
            err,
            TsinkError::Other(ref message)
                if message == "injected registry catalog cleanup sync failure"
        ));
        assert!(!catalog_entry_path(&store_path, first_removed).exists());
        assert!(catalog_entry_path(&store_path, later_removal).is_file());
        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            snapshot.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp.path()).unwrap()
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    }

    #[test]
    fn catalog_namespace_reserves_manifest_and_atomic_temporary_slots() {
        admit_catalog_namespace(
            REGISTRY_CATALOG_MAX_LIVE_ENTRIES,
            REGISTRY_CATALOG_MAX_LIVE_ENTRIES,
        )
        .unwrap();
        assert!(matches!(
            admit_catalog_namespace(
                REGISTRY_CATALOG_MAX_LIVE_ENTRIES,
                REGISTRY_CATALOG_MAX_LIVE_ENTRIES + 1,
            ),
            Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                limit: REGISTRY_CATALOG_MAX_LIVE_ENTRIES,
                required,
                ..
            }) if required == REGISTRY_CATALOG_MAX_LIVE_ENTRIES + 1
        ));
    }

    #[test]
    fn removed_catalog_key_is_recoverable_from_an_absent_canonical_root() {
        let root = Path::new("/data/numeric/segments/L2/seg-00000000000000af");
        assert_eq!(
            catalog_entry_key_from_root(SegmentLaneFamily::Numeric, root).unwrap(),
            PersistedRegistryCatalogEntryKey {
                lane: SegmentLaneFamily::Numeric,
                level: 2,
                segment_id: 0xaf,
            }
        );
        assert!(catalog_entry_key_from_root(
            SegmentLaneFamily::Numeric,
            Path::new("/data/numeric/segments/L3/seg-00000000000000af"),
        )
        .is_err());
        assert!(catalog_entry_key_from_root(
            SegmentLaneFamily::Numeric,
            Path::new("/data/numeric/segments/L2/seg-00000000000000AF"),
        )
        .is_err());
    }

    #[test]
    fn oversized_native_manifest_is_rejected_before_decode() {
        let temp = TempDir::new().unwrap();
        let snapshot_path = temp.path().join("series_index.bin");
        let store_path = catalog_store_path(&snapshot_path);
        fs::create_dir_all(&store_path).unwrap();
        let manifest_path = store_path.join(REGISTRY_CATALOG_STORE_MANIFEST_FILE_NAME);
        fs::File::create(&manifest_path)
            .unwrap()
            .set_len(REGISTRY_CATALOG_STORE_MANIFEST_MAX_BYTES + 1)
            .unwrap();

        assert!(load_store_manifest(&store_path).unwrap().is_none());
        assert!(validate_registry_catalog_store(&snapshot_path, &[])
            .unwrap()
            .is_none());
    }
}
