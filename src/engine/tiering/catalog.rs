use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::{xxh64, Xxh64};

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::inventory::SegmentInventoryAccumulator;
use super::layout::{relative_segment_path, SegmentPathResolver};
use super::{PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily};
use crate::engine::binio::{
    append_i64, append_u16, append_u32, append_u64, append_u8, checksum32, read_bytes, read_i64,
    read_u16, read_u32, read_u64, read_u8,
};
use crate::engine::fs_utils::{
    create_dir_all_and_sync_parents, is_link_or_reparse_point,
    remove_path_if_exists_and_sync_parent_budgeted_with_reconciliation_memory_limit,
    write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit,
    MAX_RECOVERY_NAMESPACE_ENTRIES,
};
use crate::engine::segment::{SegmentManifest, WalHighWatermark};

pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_FILE_NAME: &str =
    "segment_catalog.json";
const SEGMENT_CATALOG_VERSION: u32 = 2;

pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_POINTER_FILE_NAME: &str =
    "segment_catalog.current";
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_GENERATION_DIRECTORY_NAME: &str =
    "segment_catalog.d";
const SEGMENT_CATALOG_GENERATION_FILE_PREFIX: &str = "catalog-";
const SEGMENT_CATALOG_GENERATION_FILE_SUFFIX: &str = ".bin";
const SEGMENT_CATALOG_V3_VERSION: u16 = 3;
const SEGMENT_CATALOG_POINTER_MAGIC: [u8; 4] = *b"TSC3";
const SEGMENT_CATALOG_GENERATION_MAGIC: [u8; 4] = *b"TSG3";
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_POINTER_BYTES: usize = 44;
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_GENERATION_HEADER_BYTES: usize = 28;
const SEGMENT_CATALOG_FRAME_PREFIX_BYTES: usize = 8;
const SEGMENT_CATALOG_ENTRY_FIXED_BYTES: usize = 72;
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES: usize = 256;
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_MAX_ENTRIES: usize =
    MAX_RECOVERY_NAMESPACE_ENTRIES;
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_MAX_FRAME_BYTES: usize =
    SEGMENT_CATALOG_FRAME_PREFIX_BYTES
        + SEGMENT_CATALOG_ENTRY_FIXED_BYTES
        + SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES;
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_MAX_GENERATION_BYTES: u64 =
    SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64
        + SEGMENT_CATALOG_MAX_ENTRIES as u64 * SEGMENT_CATALOG_MAX_FRAME_BYTES as u64;
const SEGMENT_CATALOG_GENERATION_READ_OPERATION: &str =
    "finite remote segment catalog generation read";
const SEGMENT_CATALOG_READER_ALLOCATION_ALLOWANCE_BYTES: usize = 64;
const SEGMENT_CATALOG_POINTER_REQUIRED_OPERATION: &str = "finite remote segment catalog refresh";
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_PUBLICATION_MEMORY_OPERATION: &str =
    "read-write segment catalog publication";
const SEGMENT_CATALOG_PUBLICATION_BASE_BYTES: usize = 16 * 1024;
const SEGMENT_CATALOG_ATOMIC_WRITER_BYTES: usize = 16 * 1024;
const SEGMENT_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES: usize = 256;
// Canonical relative paths contain only fixed ASCII components. This bound also covers every
// numeric field, JSON punctuation/indentation, and serde's per-entry traversal scratch.
const SEGMENT_CATALOG_LEGACY_JSON_BYTES_PER_ENTRY: usize = 1024;
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES: usize =
    SEGMENT_CATALOG_LEGACY_STREAM_PREFIX.len()
        + SEGMENT_CATALOG_LEGACY_STREAM_SUFFIX.len()
        + SEGMENT_CATALOG_MAX_ENTRIES * SEGMENT_CATALOG_LEGACY_JSON_BYTES_PER_ENTRY;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::storage_engine) struct SegmentCatalogPointer {
    pub(in crate::engine::storage_engine) generation: u64,
    pub(in crate::engine::storage_engine) entry_count: u64,
    pub(in crate::engine::storage_engine) generation_file_len: u64,
    pub(in crate::engine::storage_engine) generation_xxh64: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::engine::storage_engine) struct SegmentCatalogIdentity {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
}

/// File-handle-free continuation for incrementally validating one immutable v3 generation.
///
/// Each call to [`read_segment_catalog_generation_page`] opens, seeks, and closes the generation
/// file. Live visibility must not consume the returned entries until `complete` is true.
pub(in crate::engine::storage_engine) struct SegmentCatalogGenerationReadCursor {
    pointer: SegmentCatalogPointer,
    offset: u64,
    entries_read: u64,
    initialized: bool,
    rolling_hash: Xxh64,
    previous_identity: Option<SegmentCatalogIdentity>,
}

impl SegmentCatalogGenerationReadCursor {
    pub(in crate::engine::storage_engine) fn new(pointer: SegmentCatalogPointer) -> Self {
        Self {
            pointer,
            offset: 0,
            entries_read: 0,
            initialized: false,
            rolling_hash: Xxh64::new(0),
            previous_identity: None,
        }
    }

    pub(in crate::engine::storage_engine) fn pointer(&self) -> SegmentCatalogPointer {
        self.pointer
    }
}

/// Conservative peak heap bytes used while decoding one v3 generation entry. The caller adds its
/// already-retained cursor/map charge and admits the sum before entering the reader.
pub(in crate::engine::storage_engine) fn modeled_segment_catalog_generation_entry_read_bytes(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> usize {
    let configured_root_bytes = numeric_lane_path
        .into_iter()
        .chain(blob_lane_path)
        .map(|path| path.as_os_str().as_encoded_bytes().len())
        .chain(tiered_storage.into_iter().map(|config| {
            config
                .object_store_root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .saturating_add("/warm/numeric".len())
        }))
        .max()
        .unwrap_or(0);
    let canonical_path_bytes = SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES
        .saturating_add(SEGMENT_CATALOG_READER_ALLOCATION_ALLOWANCE_BYTES);
    let resolved_root_bytes = configured_root_bytes
        .saturating_add(SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES)
        .saturating_add(2)
        .saturating_add(SEGMENT_CATALOG_READER_ALLOCATION_ALLOWANCE_BYTES);

    SEGMENT_CATALOG_MAX_FRAME_BYTES
        .saturating_add(std::mem::size_of::<SegmentInventoryEntry>())
        .saturating_add(configured_root_bytes)
        .saturating_add(SEGMENT_CATALOG_READER_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(canonical_path_bytes)
        .saturating_add(resolved_root_bytes)
        .saturating_add(
            segment_catalog_generation_file_name(u64::MAX)
                .len()
                .saturating_add(SEGMENT_CATALOG_READER_ALLOCATION_ALLOWANCE_BYTES),
        )
}

pub(in crate::engine::storage_engine) struct SegmentCatalogGenerationReadPage {
    pub(in crate::engine::storage_engine) entries: Vec<SegmentInventoryEntry>,
    pub(in crate::engine::storage_engine) file_bytes_read: u64,
    pub(in crate::engine::storage_engine) complete: bool,
    pub(in crate::engine::storage_engine) deferred_frame: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentCatalogFile {
    version: u32,
    entries: Vec<SegmentCatalogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentCatalogEntry {
    lane: SegmentLaneFamily,
    tier: PersistedSegmentTier,
    level: u8,
    segment_id: u64,
    #[serde(default)]
    chunk_count: usize,
    #[serde(default)]
    point_count: usize,
    #[serde(default)]
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    #[serde(default)]
    wal_highwater_segment: u64,
    #[serde(default)]
    wal_highwater_frame: u64,
    relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentCatalogPreflight {
    version: u32,
    entry_count: usize,
}

struct SegmentCatalogPreflightEntries(usize);

impl<'de> Deserialize<'de> for SegmentCatalogPreflightEntries {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EntriesVisitor;

        impl<'de> Visitor<'de> for EntriesVisitor {
            type Value = SegmentCatalogPreflightEntries;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded segment catalog entry array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut count = 0usize;
                while sequence.next_element::<IgnoredAny>()?.is_some() {
                    count = count.checked_add(1).ok_or_else(|| {
                        serde::de::Error::custom("segment catalog entry count overflow")
                    })?;
                    if count > SEGMENT_CATALOG_MAX_ENTRIES {
                        return Err(serde::de::Error::custom(format!(
                            "segment catalog has more than {SEGMENT_CATALOG_MAX_ENTRIES} entries"
                        )));
                    }
                }
                Ok(SegmentCatalogPreflightEntries(count))
            }
        }

        deserializer.deserialize_seq(EntriesVisitor)
    }
}

impl<'de> Deserialize<'de> for SegmentCatalogPreflight {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PreflightVisitor;

        impl<'de> Visitor<'de> for PreflightVisitor {
            type Value = SegmentCatalogPreflight;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a versioned segment catalog object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut version = None;
                let mut entry_count = None;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "version" => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(map.next_value::<u32>()?);
                        }
                        "entries" => {
                            if entry_count.is_some() {
                                return Err(serde::de::Error::duplicate_field("entries"));
                            }
                            entry_count =
                                Some(map.next_value::<SegmentCatalogPreflightEntries>()?.0);
                        }
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(SegmentCatalogPreflight {
                    version: version.ok_or_else(|| serde::de::Error::missing_field("version"))?,
                    entry_count: entry_count
                        .ok_or_else(|| serde::de::Error::missing_field("entries"))?,
                })
            }
        }

        deserializer.deserialize_map(PreflightVisitor)
    }
}

fn segment_catalog_atomic_write_staging_for_path_len(path_len: usize) -> usize {
    SEGMENT_CATALOG_ATOMIC_WRITER_BYTES
        // Target/tmp paths, an absolute-path normalization copy, and the ancestor vector can
        // coexist. Eight bytes per encoded path byte also covers maximally deep short components.
        .saturating_add(path_len.saturating_mul(8))
        .saturating_add(SEGMENT_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES)
}

/// Conservative complete peak for encoding and atomically replacing one readable v2 catalog.
///
/// The model includes the cloned entry vector, every owned relative-path string, geometric JSON
/// `Vec` growth, and the atomic writer/path scratch that coexist with the encoded bytes.
fn modeled_legacy_segment_catalog_publication_bytes_for_path_len(
    path_len: usize,
    inventory: &SegmentInventory,
) -> usize {
    let entries = inventory.entries().len();
    let entry_clones = entries
        .saturating_mul(std::mem::size_of::<SegmentCatalogEntry>())
        .saturating_add(
            entries.saturating_mul(
                SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES
                    .saturating_add(SEGMENT_CATALOG_READER_ALLOCATION_ALLOWANCE_BYTES),
            ),
        );
    let encoded_upper_bound = SEGMENT_CATALOG_PUBLICATION_BASE_BYTES
        .saturating_add(entries.saturating_mul(SEGMENT_CATALOG_LEGACY_JSON_BYTES_PER_ENTRY));
    // `serde_json::to_vec_pretty` grows a Vec geometrically while the cloned entries remain live.
    let encoded_capacity = encoded_upper_bound.saturating_mul(2);
    SEGMENT_CATALOG_PUBLICATION_BASE_BYTES
        .saturating_add(entry_clones)
        .saturating_add(encoded_capacity)
        .saturating_add(segment_catalog_atomic_write_staging_for_path_len(path_len))
}

pub(in crate::engine::storage_engine) fn modeled_legacy_segment_catalog_publication_bytes(
    path: &Path,
    inventory: &SegmentInventory,
) -> usize {
    modeled_legacy_segment_catalog_publication_bytes_for_path_len(
        path.as_os_str().as_encoded_bytes().len(),
        inventory,
    )
}

/// Conservative complete peak for one shared v3 generation, v2 compatibility image, and pointer.
///
/// The generation and compatibility buffers are published sequentially. Their peaks therefore
/// take a maximum rather than a sum; callers that retain a cloned shared inventory add that charge
/// separately.
pub(in crate::engine::storage_engine) fn modeled_shared_segment_catalog_publication_bytes(
    config: &TieredStorageConfig,
    inventory: &SegmentInventory,
) -> usize {
    let entries = inventory.entries().len();
    let generation_bytes = SEGMENT_CATALOG_GENERATION_HEADER_BYTES
        .saturating_add(entries.saturating_mul(SEGMENT_CATALOG_MAX_FRAME_BYTES));
    // The exact-size slice iterator allocates one pointer vector and unstable sort uses no heap
    // scratch; the publication base covers the vector allocation header and allocator metadata.
    let sorted_refs = entries.saturating_mul(std::mem::size_of::<&SegmentInventoryEntry>());
    let generation_peak = SEGMENT_CATALOG_PUBLICATION_BASE_BYTES
        .saturating_add(sorted_refs)
        // Exact preallocation is used below, but twice the logical maximum also covers any
        // allocator capacity rounding without depending on allocator internals.
        .saturating_add(generation_bytes.saturating_mul(2))
        .saturating_add(SEGMENT_CATALOG_MAX_FRAME_BYTES)
        .saturating_add(segment_catalog_atomic_write_staging_for_path_len(
            config
                .object_store_root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .saturating_add(2)
                .saturating_add(SEGMENT_CATALOG_GENERATION_DIRECTORY_NAME.len())
                .saturating_add(SEGMENT_CATALOG_GENERATION_FILE_PREFIX.len())
                .saturating_add(16)
                .saturating_add(SEGMENT_CATALOG_GENERATION_FILE_SUFFIX.len()),
        ));
    let legacy_peak = modeled_legacy_segment_catalog_publication_bytes_for_path_len(
        config
            .object_store_root
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .saturating_add(1)
            .saturating_add(SEGMENT_CATALOG_FILE_NAME.len()),
        inventory,
    );
    let pointer_peak = SEGMENT_CATALOG_PUBLICATION_BASE_BYTES
        .saturating_add(SEGMENT_CATALOG_POINTER_BYTES.saturating_mul(2))
        .saturating_add(segment_catalog_atomic_write_staging_for_path_len(
            config
                .object_store_root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .saturating_add(1)
                .saturating_add(SEGMENT_CATALOG_POINTER_FILE_NAME.len()),
        ));
    generation_peak.max(legacy_peak).max(pointer_peak)
}

#[cfg(test)]
pub(in crate::engine::storage_engine) fn persist_segment_catalog(
    path: &Path,
    inventory: &SegmentInventory,
) -> Result<()> {
    persist_segment_catalog_budgeted(path, inventory, None)
}

pub(in crate::engine::storage_engine) fn persist_segment_catalog_budgeted(
    path: &Path,
    inventory: &SegmentInventory,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    persist_segment_catalog_budgeted_with_memory_admission(
        path,
        inventory,
        local_disk_budget,
        |_| Ok(()),
    )
}

pub(in crate::engine::storage_engine) fn persist_segment_catalog_budgeted_with_memory_admission(
    path: &Path,
    inventory: &SegmentInventory,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let publication_memory_limit =
        modeled_legacy_segment_catalog_publication_bytes(path, inventory);
    admit_memory(publication_memory_limit)?;
    let bytes = encode_legacy_segment_catalog(inventory)?;
    write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
        path,
        bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        crate::DiskReservationKind::Maintenance,
        publication_memory_limit,
    )
}

fn encode_legacy_segment_catalog(inventory: &SegmentInventory) -> Result<Vec<u8>> {
    let entries = inventory
        .entries()
        .iter()
        .map(|entry| SegmentCatalogEntry {
            lane: entry.lane,
            tier: entry.tier,
            level: entry.manifest.level,
            segment_id: entry.manifest.segment_id,
            chunk_count: entry.manifest.chunk_count,
            point_count: entry.manifest.point_count,
            series_count: entry.manifest.series_count,
            min_ts: entry.manifest.min_ts,
            max_ts: entry.manifest.max_ts,
            wal_highwater_segment: entry.manifest.wal_highwater.segment,
            wal_highwater_frame: entry.manifest.wal_highwater.frame,
            relative_path: relative_segment_path(&entry.manifest)
                .to_string_lossy()
                .into_owned(),
        })
        .collect::<Vec<_>>();

    serde_json::to_vec_pretty(&SegmentCatalogFile {
        version: SEGMENT_CATALOG_VERSION,
        entries,
    })
    .map_err(Into::into)
}

pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_LEGACY_STREAM_PREFIX: &[u8] =
    b"{\"version\":2,\"entries\":[";
pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_LEGACY_STREAM_SUFFIX: &[u8] = b"]}";

/// Encodes one legacy-v2 array member without retaining the complete compatibility image.
pub(in crate::engine::storage_engine) fn encode_legacy_segment_catalog_entry_fragment(
    entry: &SegmentInventoryEntry,
    first: bool,
) -> Result<Vec<u8>> {
    let encoded = serde_json::to_vec(&SegmentCatalogEntry {
        lane: entry.lane,
        tier: entry.tier,
        level: entry.manifest.level,
        segment_id: entry.manifest.segment_id,
        chunk_count: entry.manifest.chunk_count,
        point_count: entry.manifest.point_count,
        series_count: entry.manifest.series_count,
        min_ts: entry.manifest.min_ts,
        max_ts: entry.manifest.max_ts,
        wal_highwater_segment: entry.manifest.wal_highwater.segment,
        wal_highwater_frame: entry.manifest.wal_highwater.frame,
        relative_path: relative_segment_path(&entry.manifest)
            .to_string_lossy()
            .into_owned(),
    })?;
    if first {
        return Ok(encoded);
    }
    let mut fragment = Vec::with_capacity(encoded.len().saturating_add(1));
    fragment.push(b',');
    fragment.extend_from_slice(&encoded);
    Ok(fragment)
}

pub(in crate::engine::storage_engine) fn shared_segment_catalog_path(
    config: &TieredStorageConfig,
) -> PathBuf {
    config.object_store_root.join(SEGMENT_CATALOG_FILE_NAME)
}

pub(in crate::engine::storage_engine) fn shared_segment_catalog_pointer_path(
    config: &TieredStorageConfig,
) -> PathBuf {
    config
        .object_store_root
        .join(SEGMENT_CATALOG_POINTER_FILE_NAME)
}

pub(in crate::engine::storage_engine) fn shared_segment_catalog_generation_directory(
    config: &TieredStorageConfig,
) -> PathBuf {
    config
        .object_store_root
        .join(SEGMENT_CATALOG_GENERATION_DIRECTORY_NAME)
}

pub(in crate::engine::storage_engine) fn shared_segment_catalog_generation_path(
    config: &TieredStorageConfig,
    generation: u64,
) -> PathBuf {
    shared_segment_catalog_generation_directory(config)
        .join(segment_catalog_generation_file_name(generation))
}

fn segment_catalog_generation_file_name(generation: u64) -> String {
    format!(
        "{SEGMENT_CATALOG_GENERATION_FILE_PREFIX}{generation:016x}{SEGMENT_CATALOG_GENERATION_FILE_SUFFIX}"
    )
}

fn parse_segment_catalog_generation_file_name(name: &str) -> Option<u64> {
    let encoded = name
        .strip_prefix(SEGMENT_CATALOG_GENERATION_FILE_PREFIX)?
        .strip_suffix(SEGMENT_CATALOG_GENERATION_FILE_SUFFIX)?;
    if encoded.len() != 16
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let generation = u64::from_str_radix(encoded, 16).ok()?;
    (name == segment_catalog_generation_file_name(generation)).then_some(generation)
}

pub(in crate::engine::storage_engine) fn encode_segment_catalog_pointer(
    pointer: SegmentCatalogPointer,
) -> Result<Vec<u8>> {
    validate_segment_catalog_pointer(pointer)?;
    let mut bytes = Vec::with_capacity(SEGMENT_CATALOG_POINTER_BYTES);
    bytes.extend_from_slice(&SEGMENT_CATALOG_POINTER_MAGIC);
    append_u16(&mut bytes, SEGMENT_CATALOG_V3_VERSION);
    append_u16(&mut bytes, 0);
    append_u64(&mut bytes, pointer.generation);
    append_u64(&mut bytes, pointer.entry_count);
    append_u64(&mut bytes, pointer.generation_file_len);
    append_u64(&mut bytes, pointer.generation_xxh64);
    let crc = checksum32(&bytes);
    append_u32(&mut bytes, crc);
    debug_assert_eq!(bytes.len(), SEGMENT_CATALOG_POINTER_BYTES);
    Ok(bytes)
}

fn decode_segment_catalog_pointer(bytes: &[u8]) -> Result<SegmentCatalogPointer> {
    if bytes.len() != SEGMENT_CATALOG_POINTER_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 pointer must be exactly {SEGMENT_CATALOG_POINTER_BYTES} bytes, found {}",
            bytes.len()
        )));
    }
    let expected_crc = checksum32(&bytes[..SEGMENT_CATALOG_POINTER_BYTES - 4]);
    let mut pos = 0usize;
    let magic = read_bytes(bytes, &mut pos, SEGMENT_CATALOG_POINTER_MAGIC.len())?;
    if magic != SEGMENT_CATALOG_POINTER_MAGIC {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 pointer magic mismatch".to_string(),
        ));
    }
    let version = read_u16(bytes, &mut pos)?;
    if version != SEGMENT_CATALOG_V3_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported segment catalog pointer version {version}"
        )));
    }
    let reserved = read_u16(bytes, &mut pos)?;
    if reserved != 0 {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 pointer reserved bits are non-zero".to_string(),
        ));
    }
    let pointer = SegmentCatalogPointer {
        generation: read_u64(bytes, &mut pos)?,
        entry_count: read_u64(bytes, &mut pos)?,
        generation_file_len: read_u64(bytes, &mut pos)?,
        generation_xxh64: read_u64(bytes, &mut pos)?,
    };
    let actual_crc = read_u32(bytes, &mut pos)?;
    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 pointer has trailing bytes".to_string(),
        ));
    }
    if actual_crc != expected_crc {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 pointer checksum mismatch".to_string(),
        ));
    }
    validate_segment_catalog_pointer(pointer)?;
    Ok(pointer)
}

fn validate_segment_catalog_pointer(pointer: SegmentCatalogPointer) -> Result<()> {
    if pointer.generation == 0 {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 pointer generation must be non-zero".to_string(),
        ));
    }
    let entry_count = usize::try_from(pointer.entry_count).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "segment catalog v3 pointer entry count {} cannot fit in memory",
            pointer.entry_count
        ))
    })?;
    if entry_count > SEGMENT_CATALOG_MAX_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: SEGMENT_CATALOG_GENERATION_READ_OPERATION,
            limit: SEGMENT_CATALOG_MAX_ENTRIES,
            required: entry_count,
        });
    }
    let minimum_len = (SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64).saturating_add(
        pointer.entry_count.saturating_mul(
            (SEGMENT_CATALOG_FRAME_PREFIX_BYTES + SEGMENT_CATALOG_ENTRY_FIXED_BYTES) as u64,
        ),
    );
    let maximum_len = (SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64).saturating_add(
        pointer
            .entry_count
            .saturating_mul(SEGMENT_CATALOG_MAX_FRAME_BYTES as u64),
    );
    if pointer.generation_file_len < minimum_len
        || pointer.generation_file_len > maximum_len
        || pointer.generation_file_len > SEGMENT_CATALOG_MAX_GENERATION_BYTES
    {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 generation length {} is outside the bounded range {minimum_len}..={maximum_len} for {} entries",
            pointer.generation_file_len, pointer.entry_count
        )));
    }
    Ok(())
}

pub(in crate::engine::storage_engine) fn load_shared_segment_catalog_pointer(
    config: &TieredStorageConfig,
) -> Result<Option<SegmentCatalogPointer>> {
    load_segment_catalog_pointer(&shared_segment_catalog_pointer_path(config))
}

pub(in crate::engine::storage_engine) fn require_shared_segment_catalog_pointer(
    config: &TieredStorageConfig,
) -> Result<SegmentCatalogPointer> {
    load_shared_segment_catalog_pointer(config)?.ok_or_else(|| TsinkError::UnsupportedOperation {
        operation: SEGMENT_CATALOG_POINTER_REQUIRED_OPERATION,
        reason: format!(
            "the authoritative v3 pointer {} is missing; finite compute-only refresh will not fall back to the legacy v2 catalog or scan remote tier roots",
            shared_segment_catalog_pointer_path(config).display()
        ),
    })
}

fn load_segment_catalog_pointer(path: &Path) -> Result<Option<SegmentCatalogPointer>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 pointer is not a regular no-follow file: {}",
            path.display()
        )));
    }
    if metadata.len() != SEGMENT_CATALOG_POINTER_BYTES as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 pointer must be exactly {SEGMENT_CATALOG_POINTER_BYTES} bytes, found {} at {}",
            metadata.len(),
            path.display()
        )));
    }
    let mut file = open_regular_file_no_follow(path, "segment catalog v3 pointer")?;
    let mut bytes = [0u8; SEGMENT_CATALOG_POINTER_BYTES];
    read_exact_catalog_bytes(&mut file, &mut bytes, path, "segment catalog v3 pointer")?;
    let mut trailing = [0u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?
        != 0
    {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 pointer changed length while reading: {}",
            path.display()
        )));
    }
    decode_segment_catalog_pointer(&bytes).map(Some)
}

fn open_regular_file_no_follow(path: &Path, description: &str) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "{description} is not a regular no-follow file: {}",
            path.display()
        )));
    }
    Ok(file)
}

fn read_exact_catalog_bytes(
    file: &mut File,
    bytes: &mut [u8],
    path: &Path,
    description: &str,
) -> Result<()> {
    file.read_exact(bytes).map_err(|source| {
        if source.kind() == std::io::ErrorKind::UnexpectedEof {
            TsinkError::DataCorruption(format!(
                "{description} was truncated while reading {} bytes from {}",
                bytes.len(),
                path.display()
            ))
        } else {
            TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            }
        }
    })
}

fn encode_segment_catalog_generation(
    inventory: &SegmentInventory,
    generation: u64,
) -> Result<(Vec<u8>, SegmentCatalogPointer)> {
    if generation == 0 {
        return Err(TsinkError::InvalidConfiguration(
            "segment catalog generation must be non-zero".to_string(),
        ));
    }
    if inventory.entries().len() > SEGMENT_CATALOG_MAX_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "segment catalog v3 generation publication",
            limit: SEGMENT_CATALOG_MAX_ENTRIES,
            required: inventory.entries().len(),
        });
    }
    let entry_count = u64::try_from(inventory.entries().len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog entry count cannot fit in the v3 format".to_string(),
        )
    })?;
    let mut sorted_entries = inventory.entries().iter().collect::<Vec<_>>();
    sorted_entries.sort_unstable_by_key(|entry| SegmentCatalogIdentity {
        lane: entry.lane,
        level: entry.manifest.level,
        segment_id: entry.manifest.segment_id,
    });

    let generation_capacity =
        sorted_entries
            .iter()
            .fold(SEGMENT_CATALOG_GENERATION_HEADER_BYTES, |total, entry| {
                total
                    .saturating_add(SEGMENT_CATALOG_FRAME_PREFIX_BYTES)
                    .saturating_add(SEGMENT_CATALOG_ENTRY_FIXED_BYTES)
                    .saturating_add(
                        relative_segment_path(&entry.manifest)
                            .as_os_str()
                            .as_encoded_bytes()
                            .len(),
                    )
            });
    if u64::try_from(generation_capacity).unwrap_or(u64::MAX) > SEGMENT_CATALOG_MAX_GENERATION_BYTES
    {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "segment catalog v3 generation publication",
            limit: SEGMENT_CATALOG_MAX_GENERATION_BYTES,
            required: u64::try_from(generation_capacity).unwrap_or(u64::MAX),
        });
    }
    let mut bytes = encode_segment_catalog_generation_header(generation, entry_count)?;
    bytes.reserve(generation_capacity.saturating_sub(bytes.len()));

    let mut previous_identity = None;
    for entry in sorted_entries {
        let identity = SegmentCatalogIdentity {
            lane: entry.lane,
            level: entry.manifest.level,
            segment_id: entry.manifest.segment_id,
        };
        if previous_identity.is_some_and(|previous| previous >= identity) {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog publication contains duplicate identity {:?}",
                identity
            )));
        }
        previous_identity = Some(identity);
        let (_, frame) = encode_segment_catalog_generation_frame(entry)?;
        bytes.extend_from_slice(&frame);
    }
    let generation_file_len = u64::try_from(bytes.len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog v3 generation length cannot fit in u64".to_string(),
        )
    })?;
    if generation_file_len > SEGMENT_CATALOG_MAX_GENERATION_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "segment catalog v3 generation publication",
            limit: SEGMENT_CATALOG_MAX_GENERATION_BYTES,
            required: generation_file_len,
        });
    }
    let pointer = SegmentCatalogPointer {
        generation,
        entry_count,
        generation_file_len,
        generation_xxh64: xxh64(&bytes, 0),
    };
    validate_segment_catalog_pointer(pointer)?;
    Ok((bytes, pointer))
}

pub(in crate::engine::storage_engine) fn encode_segment_catalog_generation_header(
    generation: u64,
    entry_count: u64,
) -> Result<Vec<u8>> {
    if generation == 0 {
        return Err(TsinkError::InvalidConfiguration(
            "segment catalog generation must be non-zero".to_string(),
        ));
    }
    if usize::try_from(entry_count).unwrap_or(usize::MAX) > SEGMENT_CATALOG_MAX_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "segment catalog v3 generation publication",
            limit: SEGMENT_CATALOG_MAX_ENTRIES,
            required: usize::try_from(entry_count).unwrap_or(usize::MAX),
        });
    }
    let mut bytes = Vec::with_capacity(SEGMENT_CATALOG_GENERATION_HEADER_BYTES);
    bytes.extend_from_slice(&SEGMENT_CATALOG_GENERATION_MAGIC);
    append_u16(&mut bytes, SEGMENT_CATALOG_V3_VERSION);
    append_u16(&mut bytes, 0);
    append_u64(&mut bytes, generation);
    append_u64(&mut bytes, entry_count);
    let header_crc = checksum32(&bytes);
    append_u32(&mut bytes, header_crc);
    debug_assert_eq!(bytes.len(), SEGMENT_CATALOG_GENERATION_HEADER_BYTES);
    Ok(bytes)
}

pub(in crate::engine::storage_engine) fn encode_segment_catalog_generation_frame(
    entry: &SegmentInventoryEntry,
) -> Result<(SegmentCatalogIdentity, Vec<u8>)> {
    let identity = SegmentCatalogIdentity {
        lane: entry.lane,
        level: entry.manifest.level,
        segment_id: entry.manifest.segment_id,
    };
    let payload = encode_segment_catalog_entry(entry)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog v3 frame exceeds the u32 length field".to_string(),
        )
    })?;
    let mut frame =
        Vec::with_capacity(SEGMENT_CATALOG_FRAME_PREFIX_BYTES.saturating_add(payload.len()));
    append_u32(&mut frame, payload_len);
    append_u32(&mut frame, checksum32(&payload));
    frame.extend_from_slice(&payload);
    Ok((identity, frame))
}

pub(in crate::engine::storage_engine) fn finalized_segment_catalog_pointer(
    generation: u64,
    entry_count: usize,
    generation_file_len: u64,
    generation_xxh64: u64,
) -> Result<SegmentCatalogPointer> {
    let pointer = SegmentCatalogPointer {
        generation,
        entry_count: u64::try_from(entry_count).map_err(|_| {
            TsinkError::InvalidConfiguration(
                "segment catalog entry count cannot fit in the v3 format".to_string(),
            )
        })?,
        generation_file_len,
        generation_xxh64,
    };
    validate_segment_catalog_pointer(pointer)?;
    Ok(pointer)
}

fn encode_segment_catalog_entry(entry: &SegmentInventoryEntry) -> Result<Vec<u8>> {
    if entry.manifest.level > 2 {
        return Err(TsinkError::InvalidConfiguration(format!(
            "segment catalog entry uses unsupported level L{}",
            entry.manifest.level
        )));
    }
    if matches!(
        (entry.manifest.min_ts, entry.manifest.max_ts),
        (Some(min_ts), Some(max_ts)) if min_ts > max_ts
    ) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "segment catalog entry {} has minimum timestamp greater than maximum timestamp",
            entry.manifest.segment_id
        )));
    }
    let relative_path = relative_segment_path(&entry.manifest);
    let relative_path = relative_path.to_str().ok_or_else(|| {
        TsinkError::InvalidConfiguration(
            "canonical segment catalog relative path is not valid UTF-8".to_string(),
        )
    })?;
    if relative_path.len() > SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "segment catalog v3 relative path",
            limit: SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES as u64,
            required: u64::try_from(relative_path.len()).unwrap_or(u64::MAX),
        });
    }
    let path_len = u32::try_from(relative_path.len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog v3 relative path cannot fit in u32".to_string(),
        )
    })?;
    let chunk_count = u64::try_from(entry.manifest.chunk_count).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog chunk count cannot fit in the v3 format".to_string(),
        )
    })?;
    let point_count = u64::try_from(entry.manifest.point_count).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog point count cannot fit in the v3 format".to_string(),
        )
    })?;
    let series_count = u64::try_from(entry.manifest.series_count).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "segment catalog series count cannot fit in the v3 format".to_string(),
        )
    })?;

    let mut flags = 0u8;
    if entry.manifest.min_ts.is_some() {
        flags |= 0b0000_0001;
    }
    if entry.manifest.max_ts.is_some() {
        flags |= 0b0000_0010;
    }
    let mut payload =
        Vec::with_capacity(SEGMENT_CATALOG_ENTRY_FIXED_BYTES.saturating_add(relative_path.len()));
    append_u8(
        &mut payload,
        match entry.lane {
            SegmentLaneFamily::Numeric => 0,
            SegmentLaneFamily::Blob => 1,
        },
    );
    append_u8(
        &mut payload,
        match entry.tier {
            PersistedSegmentTier::Hot => 0,
            PersistedSegmentTier::Warm => 1,
            PersistedSegmentTier::Cold => 2,
        },
    );
    append_u8(&mut payload, entry.manifest.level);
    append_u8(&mut payload, flags);
    append_u64(&mut payload, entry.manifest.segment_id);
    append_u64(&mut payload, chunk_count);
    append_u64(&mut payload, point_count);
    append_u64(&mut payload, series_count);
    append_i64(&mut payload, entry.manifest.min_ts.unwrap_or(0));
    append_i64(&mut payload, entry.manifest.max_ts.unwrap_or(0));
    append_u64(&mut payload, entry.manifest.wal_highwater.segment);
    append_u64(&mut payload, entry.manifest.wal_highwater.frame);
    append_u32(&mut payload, path_len);
    payload.extend_from_slice(relative_path.as_bytes());
    debug_assert_eq!(
        payload.len(),
        SEGMENT_CATALOG_ENTRY_FIXED_BYTES + relative_path.len()
    );
    Ok(payload)
}

fn decode_segment_catalog_entry(
    payload: &[u8],
    resolver: SegmentPathResolver<'_>,
) -> Result<(SegmentCatalogIdentity, SegmentInventoryEntry)> {
    if payload.len() < SEGMENT_CATALOG_ENTRY_FIXED_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 frame is shorter than the fixed entry size: {}",
            payload.len()
        )));
    }
    if payload.len() > SEGMENT_CATALOG_ENTRY_FIXED_BYTES + SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "segment catalog v3 frame",
            limit: SEGMENT_CATALOG_MAX_FRAME_BYTES as u64,
            required: u64::try_from(payload.len() + SEGMENT_CATALOG_FRAME_PREFIX_BYTES)
                .unwrap_or(u64::MAX),
        });
    }
    let mut pos = 0usize;
    let lane = match read_u8(payload, &mut pos)? {
        0 => SegmentLaneFamily::Numeric,
        1 => SegmentLaneFamily::Blob,
        value => {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 entry uses unknown lane value {value}"
            )))
        }
    };
    let tier = match read_u8(payload, &mut pos)? {
        0 => PersistedSegmentTier::Hot,
        1 => PersistedSegmentTier::Warm,
        2 => PersistedSegmentTier::Cold,
        value => {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 entry uses unknown tier value {value}"
            )))
        }
    };
    let level = read_u8(payload, &mut pos)?;
    if level > 2 {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 entry uses unsupported level L{level}"
        )));
    }
    let flags = read_u8(payload, &mut pos)?;
    if flags & !0b0000_0011 != 0 {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 entry uses unknown flags 0x{flags:02x}"
        )));
    }
    let segment_id = read_u64(payload, &mut pos)?;
    let chunk_count_u64 = read_u64(payload, &mut pos)?;
    let point_count_u64 = read_u64(payload, &mut pos)?;
    let series_count_u64 = read_u64(payload, &mut pos)?;
    let encoded_min_ts = read_i64(payload, &mut pos)?;
    let encoded_max_ts = read_i64(payload, &mut pos)?;
    if flags & 0b0000_0001 == 0 && encoded_min_ts != 0 {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 entry has a non-canonical absent minimum timestamp".to_string(),
        ));
    }
    if flags & 0b0000_0010 == 0 && encoded_max_ts != 0 {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 entry has a non-canonical absent maximum timestamp".to_string(),
        ));
    }
    let min_ts = (flags & 0b0000_0001 != 0).then_some(encoded_min_ts);
    let max_ts = (flags & 0b0000_0010 != 0).then_some(encoded_max_ts);
    if matches!((min_ts, max_ts), (Some(min_ts), Some(max_ts)) if min_ts > max_ts) {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 entry {segment_id} has minimum timestamp greater than maximum timestamp"
        )));
    }
    let wal_highwater_segment = read_u64(payload, &mut pos)?;
    let wal_highwater_frame = read_u64(payload, &mut pos)?;
    let relative_path_len = usize::try_from(read_u32(payload, &mut pos)?).map_err(|_| {
        TsinkError::DataCorruption(
            "segment catalog v3 relative path length cannot fit in memory".to_string(),
        )
    })?;
    if relative_path_len > SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "segment catalog v3 relative path",
            limit: SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES as u64,
            required: u64::try_from(relative_path_len).unwrap_or(u64::MAX),
        });
    }
    let path_bytes = read_bytes(payload, &mut pos, relative_path_len)?;
    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 entry has trailing payload bytes".to_string(),
        ));
    }
    let relative_path = std::str::from_utf8(path_bytes).map_err(|_| {
        TsinkError::DataCorruption(
            "segment catalog v3 relative path is not valid UTF-8".to_string(),
        )
    })?;
    let chunk_count = usize::try_from(chunk_count_u64).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "segment catalog v3 chunk count {chunk_count_u64} cannot fit in memory"
        ))
    })?;
    let point_count = usize::try_from(point_count_u64).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "segment catalog v3 point count {point_count_u64} cannot fit in memory"
        ))
    })?;
    let series_count = usize::try_from(series_count_u64).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "segment catalog v3 series count {series_count_u64} cannot fit in memory"
        ))
    })?;
    let manifest = SegmentManifest {
        segment_id,
        level,
        chunk_count,
        point_count,
        series_count,
        min_ts,
        max_ts,
        wal_highwater: WalHighWatermark {
            segment: wal_highwater_segment,
            frame: wal_highwater_frame,
        },
    };
    let canonical = relative_segment_path(&manifest);
    if Path::new(relative_path) != canonical {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 relative path {relative_path} did not match canonical path {}",
            canonical.display()
        )));
    }
    let base_path = resolver.catalog_lane_root(lane, tier)?;
    let identity = SegmentCatalogIdentity {
        lane,
        level,
        segment_id,
    };
    Ok((
        identity,
        SegmentInventoryEntry {
            lane,
            tier,
            root: base_path.join(canonical),
            manifest,
        },
    ))
}

fn decode_segment_catalog_generation_header(
    bytes: &[u8],
    pointer: SegmentCatalogPointer,
) -> Result<()> {
    if bytes.len() != SEGMENT_CATALOG_GENERATION_HEADER_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 generation header must be exactly {SEGMENT_CATALOG_GENERATION_HEADER_BYTES} bytes"
        )));
    }
    let expected_crc = checksum32(&bytes[..SEGMENT_CATALOG_GENERATION_HEADER_BYTES - 4]);
    let mut pos = 0usize;
    if read_bytes(bytes, &mut pos, SEGMENT_CATALOG_GENERATION_MAGIC.len())?
        != SEGMENT_CATALOG_GENERATION_MAGIC
    {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 generation magic mismatch".to_string(),
        ));
    }
    let version = read_u16(bytes, &mut pos)?;
    if version != SEGMENT_CATALOG_V3_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported segment catalog generation version {version}"
        )));
    }
    if read_u16(bytes, &mut pos)? != 0 {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 generation reserved bits are non-zero".to_string(),
        ));
    }
    let generation = read_u64(bytes, &mut pos)?;
    let entry_count = read_u64(bytes, &mut pos)?;
    let actual_crc = read_u32(bytes, &mut pos)?;
    if actual_crc != expected_crc {
        return Err(TsinkError::DataCorruption(
            "segment catalog v3 generation header checksum mismatch".to_string(),
        ));
    }
    if generation != pointer.generation || entry_count != pointer.entry_count {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog v3 generation header ({generation}, {entry_count}) does not match pointer ({}, {})",
            pointer.generation, pointer.entry_count
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::engine::storage_engine) fn read_segment_catalog_generation_page(
    cursor: &mut SegmentCatalogGenerationReadCursor,
    generation_path: &Path,
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
    item_limit: usize,
    byte_limit: u64,
) -> Result<SegmentCatalogGenerationReadPage> {
    if item_limit == 0 {
        return Err(TsinkError::MaintenanceDependencyWindowExceeded {
            operation: SEGMENT_CATALOG_GENERATION_READ_OPERATION,
            item_limit,
            byte_limit,
            selected_items: 0,
            selected_bytes: 0,
        });
    }
    let expected_name = segment_catalog_generation_file_name(cursor.pointer.generation);
    if generation_path.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog generation path is not canonical for generation {}: {}",
            cursor.pointer.generation,
            generation_path.display()
        )));
    }
    let generation_directory = generation_path.parent().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "segment catalog generation path has no parent: {}",
            generation_path.display()
        ))
    })?;
    let directory_metadata = std::fs::symlink_metadata(generation_directory).map_err(|source| {
        TsinkError::IoWithPath {
            path: generation_directory.to_path_buf(),
            source,
        }
    })?;
    if is_link_or_reparse_point(&directory_metadata) || !directory_metadata.file_type().is_dir() {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog generation namespace is not a regular no-follow directory: {}",
            generation_directory.display()
        )));
    }
    validate_segment_catalog_pointer(cursor.pointer)?;
    let metadata =
        std::fs::symlink_metadata(generation_path).map_err(|source| TsinkError::IoWithPath {
            path: generation_path.to_path_buf(),
            source,
        })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog generation is not a regular no-follow file: {}",
            generation_path.display()
        )));
    }
    if metadata.len() != cursor.pointer.generation_file_len {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog generation length {} does not match pointer length {} at {}",
            metadata.len(),
            cursor.pointer.generation_file_len,
            generation_path.display()
        )));
    }
    let mut file =
        open_regular_file_no_follow(generation_path, "segment catalog v3 immutable generation")?;
    let opened_len = file
        .metadata()
        .map_err(|source| TsinkError::IoWithPath {
            path: generation_path.to_path_buf(),
            source,
        })?
        .len();
    if opened_len != cursor.pointer.generation_file_len {
        return Err(TsinkError::DataCorruption(format!(
            "opened segment catalog generation length {opened_len} does not match pointer length {} at {}",
            cursor.pointer.generation_file_len,
            generation_path.display()
        )));
    }
    file.seek(SeekFrom::Start(cursor.offset))
        .map_err(|source| TsinkError::IoWithPath {
            path: generation_path.to_path_buf(),
            source,
        })?;

    let remaining_entries = usize::try_from(
        cursor
            .pointer
            .entry_count
            .saturating_sub(cursor.entries_read),
    )
    .unwrap_or(usize::MAX);
    let header_bytes = if cursor.initialized {
        0
    } else {
        SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64
    };
    let minimum_frame_bytes =
        u64::try_from(SEGMENT_CATALOG_FRAME_PREFIX_BYTES + SEGMENT_CATALOG_ENTRY_FIXED_BYTES)
            .unwrap_or(u64::MAX);
    let entries_fitting_byte_limit = byte_limit
        .saturating_sub(header_bytes)
        .checked_div(minimum_frame_bytes)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(usize::MAX);
    let page_capacity = remaining_entries
        .min(item_limit)
        .min(entries_fitting_byte_limit);
    let mut page = SegmentCatalogGenerationReadPage {
        // Callers that enforce a storage-memory envelope reserve this exact element storage
        // before entry. Preallocation also prevents Vec's geometric growth from escaping that
        // model while decoded roots are transferred into the retained catalog map.
        entries: Vec::with_capacity(page_capacity),
        file_bytes_read: 0,
        complete: false,
        deferred_frame: false,
    };
    if !cursor.initialized {
        let required = SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64;
        if required > byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: SEGMENT_CATALOG_GENERATION_READ_OPERATION,
                limit: byte_limit,
                required,
            });
        }
        let mut header = [0u8; SEGMENT_CATALOG_GENERATION_HEADER_BYTES];
        read_exact_catalog_bytes(
            &mut file,
            &mut header,
            generation_path,
            "segment catalog v3 generation header",
        )?;
        decode_segment_catalog_generation_header(&header, cursor.pointer)?;
        cursor.rolling_hash.update(&header);
        cursor.offset = required;
        cursor.initialized = true;
        page.file_bytes_read = required;
    }

    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    while cursor.entries_read < cursor.pointer.entry_count && page.entries.len() < item_limit {
        let remaining_bytes = byte_limit.saturating_sub(page.file_bytes_read);
        if remaining_bytes < SEGMENT_CATALOG_FRAME_PREFIX_BYTES as u64 {
            break;
        }
        let mut prefix = [0u8; SEGMENT_CATALOG_FRAME_PREFIX_BYTES];
        read_exact_catalog_bytes(
            &mut file,
            &mut prefix,
            generation_path,
            "segment catalog v3 frame prefix",
        )?;
        page.file_bytes_read = page
            .file_bytes_read
            .saturating_add(SEGMENT_CATALOG_FRAME_PREFIX_BYTES as u64);
        let mut prefix_pos = 0usize;
        let payload_len = usize::try_from(read_u32(&prefix, &mut prefix_pos)?).map_err(|_| {
            TsinkError::DataCorruption(
                "segment catalog v3 frame length cannot fit in memory".to_string(),
            )
        })?;
        let expected_crc = read_u32(&prefix, &mut prefix_pos)?;
        if payload_len < SEGMENT_CATALOG_ENTRY_FIXED_BYTES
            || payload_len
                > SEGMENT_CATALOG_ENTRY_FIXED_BYTES
                    .saturating_add(SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES)
        {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 frame payload length {payload_len} is outside the bounded range {}..={}",
                SEGMENT_CATALOG_ENTRY_FIXED_BYTES,
                SEGMENT_CATALOG_ENTRY_FIXED_BYTES + SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES
            )));
        }
        let frame_bytes = SEGMENT_CATALOG_FRAME_PREFIX_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "segment catalog v3 frame length overflowed usize".to_string(),
                )
            })?;
        let frame_bytes_u64 = u64::try_from(frame_bytes).unwrap_or(u64::MAX);
        if frame_bytes_u64 > byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: SEGMENT_CATALOG_GENERATION_READ_OPERATION,
                limit: byte_limit,
                required: frame_bytes_u64,
            });
        }
        if frame_bytes_u64 > remaining_bytes {
            page.deferred_frame = true;
            break;
        }
        let mut payload = vec![0u8; payload_len];
        read_exact_catalog_bytes(
            &mut file,
            &mut payload,
            generation_path,
            "segment catalog v3 frame payload",
        )?;
        if checksum32(&payload) != expected_crc {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 frame {} checksum mismatch",
                cursor.entries_read
            )));
        }
        let (identity, entry) = decode_segment_catalog_entry(&payload, resolver)?;
        if cursor
            .previous_identity
            .is_some_and(|previous| previous >= identity)
        {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 entries are duplicated or not strictly ordered at {:?}",
                identity
            )));
        }
        cursor.rolling_hash.update(&prefix);
        cursor.rolling_hash.update(&payload);
        cursor.previous_identity = Some(identity);
        cursor.entries_read = cursor.entries_read.saturating_add(1);
        cursor.offset = cursor.offset.saturating_add(frame_bytes_u64);
        page.file_bytes_read = page
            .file_bytes_read
            .saturating_add(u64::try_from(payload_len).unwrap_or(u64::MAX));
        page.entries.push(entry);
    }

    if cursor.entries_read == cursor.pointer.entry_count {
        if cursor.offset != cursor.pointer.generation_file_len {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 generation count ended at byte {}, pointer declares {}",
                cursor.offset, cursor.pointer.generation_file_len
            )));
        }
        let actual_hash = cursor.rolling_hash.digest();
        if actual_hash != cursor.pointer.generation_xxh64 {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 generation hash mismatch: expected {:016x}, found {actual_hash:016x}",
                cursor.pointer.generation_xxh64
            )));
        }
        page.complete = true;
    }
    Ok(page)
}

pub(in crate::engine::storage_engine) fn ensure_segment_catalog_generation_directory(
    path: &Path,
) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !is_link_or_reparse_point(&metadata) && metadata.file_type().is_dir() => {
            Ok(())
        }
        Ok(_) => Err(TsinkError::DataCorruption(format!(
            "segment catalog generation namespace is not a regular directory: {}",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            create_dir_all_and_sync_parents(path)?;
            let metadata =
                std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
                    path: path.to_path_buf(),
                    source,
                })?;
            if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
                return Err(TsinkError::DataCorruption(format!(
                    "segment catalog generation namespace is not a regular directory: {}",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(source) => Err(TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub(in crate::engine::storage_engine) fn count_segment_catalog_generation_namespace(
    path: &Path,
) -> Result<usize> {
    let mut count = 0usize;
    let reader = std::fs::read_dir(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    for entry in reader {
        let _ = entry.map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
        count = count.saturating_add(1);
        if count > MAX_RECOVERY_NAMESPACE_ENTRIES {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "segment catalog generation namespace",
                limit: MAX_RECOVERY_NAMESPACE_ENTRIES,
                required: count,
            });
        }
    }
    Ok(count)
}

pub(in crate::engine::storage_engine) fn cleanup_old_segment_catalog_generations(
    directory: &Path,
    current_generation: Option<u64>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reconciliation_memory_limit: usize,
) -> Result<()> {
    ensure_segment_catalog_generation_directory(directory)?;
    let _ = count_segment_catalog_generation_namespace(directory)?;
    let reader = std::fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in reader {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(generation) = parse_segment_catalog_generation_file_name(&name) else {
            continue;
        };
        if Some(generation) == current_generation {
            continue;
        }
        let file_type = entry.file_type().map_err(|source| TsinkError::IoWithPath {
            path: entry.path(),
            source,
        })?;
        if file_type.is_symlink() || !file_type.is_file() {
            continue;
        }
        remove_path_if_exists_and_sync_parent_budgeted_with_reconciliation_memory_limit(
            &entry.path(),
            local_disk_budget,
            crate::DiskCategory::Registry,
            reconciliation_memory_limit,
        )?;
    }
    Ok(())
}

fn choose_next_segment_catalog_generation(
    directory: &Path,
    current: Option<SegmentCatalogPointer>,
) -> Result<u64> {
    let namespace_count = count_segment_catalog_generation_namespace(directory)?;
    choose_next_segment_catalog_generation_with_namespace_count(directory, current, namespace_count)
}

pub(in crate::engine::storage_engine) fn choose_next_segment_catalog_generation_with_namespace_count(
    directory: &Path,
    current: Option<SegmentCatalogPointer>,
    namespace_count: usize,
) -> Result<u64> {
    if namespace_count >= MAX_RECOVERY_NAMESPACE_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "segment catalog generation publication",
            limit: MAX_RECOVERY_NAMESPACE_ENTRIES,
            required: namespace_count.saturating_add(1),
        });
    }
    let wall_clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64;
    let after_current = match current {
        Some(pointer) => pointer.generation.checked_add(1).ok_or_else(|| {
            TsinkError::DataCorruption(
                "segment catalog generation counter is exhausted".to_string(),
            )
        })?,
        None => 1,
    };
    let mut candidate = wall_clock.max(after_current).max(1);
    for _ in 0..=namespace_count {
        let path = directory.join(segment_catalog_generation_file_name(candidate));
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                candidate = candidate.checked_add(1).ok_or_else(|| {
                    TsinkError::DataCorruption(
                        "segment catalog generation counter is exhausted".to_string(),
                    )
                })?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
            Err(source) => return Err(TsinkError::IoWithPath { path, source }),
        }
    }
    Err(TsinkError::MaintenanceNamespaceLimitExceeded {
        operation: "segment catalog generation publication",
        limit: MAX_RECOVERY_NAMESPACE_ENTRIES,
        required: namespace_count.saturating_add(1),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::storage_engine) enum SegmentCatalogPublishStage {
    Generation,
    LegacyV2,
    PointerPrePublication,
    Pointer,
}

#[allow(dead_code)]
pub(in crate::engine::storage_engine) fn persist_shared_segment_catalog_budgeted(
    config: &TieredStorageConfig,
    inventory: &SegmentInventory,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<SegmentCatalogPointer> {
    persist_shared_segment_catalog_budgeted_with_memory_admission(
        config,
        inventory,
        local_disk_budget,
        |_| Ok(()),
    )
}

#[allow(dead_code)]
pub(in crate::engine::storage_engine) fn persist_shared_segment_catalog_budgeted_with_memory_admission(
    config: &TieredStorageConfig,
    inventory: &SegmentInventory,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<SegmentCatalogPointer> {
    persist_shared_segment_catalog_budgeted_with_stage_hook(
        config,
        inventory,
        local_disk_budget,
        admit_memory,
        |_| Ok(()),
    )
}

pub(in crate::engine::storage_engine) fn persist_shared_segment_catalog_budgeted_with_stage_hook<
    F,
    A,
>(
    config: &TieredStorageConfig,
    inventory: &SegmentInventory,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: A,
    mut stage_hook: F,
) -> Result<SegmentCatalogPointer>
where
    F: FnMut(SegmentCatalogPublishStage) -> Result<()>,
    A: FnMut(usize) -> Result<()>,
{
    let publication_memory_limit =
        modeled_shared_segment_catalog_publication_bytes(config, inventory);
    admit_memory(publication_memory_limit)?;
    let pointer_path = shared_segment_catalog_pointer_path(config);
    let current_pointer = load_segment_catalog_pointer(&pointer_path)?;
    let generation_directory = shared_segment_catalog_generation_directory(config);
    ensure_segment_catalog_generation_directory(&generation_directory)?;

    if let Err(err) = cleanup_old_segment_catalog_generations(
        &generation_directory,
        current_pointer.map(|pointer| pointer.generation),
        local_disk_budget,
        publication_memory_limit,
    ) {
        tracing::warn!(
            error = %err,
            path = %generation_directory.display(),
            "retryable cleanup of old segment catalog generations failed before publication"
        );
    }
    let generation =
        choose_next_segment_catalog_generation(&generation_directory, current_pointer)?;
    let (generation_bytes, pointer) = encode_segment_catalog_generation(inventory, generation)?;
    let generation_path =
        generation_directory.join(segment_catalog_generation_file_name(generation));
    write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
        &generation_path,
        generation_bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        crate::DiskReservationKind::Maintenance,
        publication_memory_limit,
    )?;
    stage_hook(SegmentCatalogPublishStage::Generation)?;

    // Compatibility publication deliberately precedes the v3 commit pointer. Existing binaries
    // keep reading the v2 snapshot, while finite readers remain pinned to the prior v3 generation.
    persist_segment_catalog_budgeted_with_memory_admission(
        &shared_segment_catalog_path(config),
        inventory,
        local_disk_budget,
        &mut admit_memory,
    )?;
    stage_hook(SegmentCatalogPublishStage::LegacyV2)?;

    let pointer_bytes = encode_segment_catalog_pointer(pointer)?;
    stage_hook(SegmentCatalogPublishStage::PointerPrePublication)?;
    write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
        &pointer_path,
        pointer_bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        crate::DiskReservationKind::Maintenance,
        publication_memory_limit,
    )?;
    stage_hook(SegmentCatalogPublishStage::Pointer)?;

    if let Err(err) = cleanup_old_segment_catalog_generations(
        &generation_directory,
        Some(pointer.generation),
        local_disk_budget,
        publication_memory_limit,
    ) {
        tracing::warn!(
            error = %err,
            path = %generation_directory.display(),
            generation = pointer.generation,
            "retryable cleanup of old segment catalog generations failed after publication"
        );
    }
    Ok(pointer)
}

pub(in crate::engine::storage_engine) fn load_segment_catalog(
    path: &Path,
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<SegmentInventory> {
    let bytes = read_legacy_segment_catalog_bounded(path)?;
    let preflight: SegmentCatalogPreflight = serde_json::from_slice(&bytes)?;
    if !(1..=SEGMENT_CATALOG_VERSION).contains(&preflight.version) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "unsupported segment catalog version {} at {}",
            preflight.version,
            path.display()
        )));
    }
    let file: SegmentCatalogFile = serde_json::from_slice(&bytes)?;
    if file.entries.len() != preflight.entry_count {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog preflight counted {} entries but typed decode produced {} at {}",
            preflight.entry_count,
            file.entries.len(),
            path.display()
        )));
    }

    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();
    for entry in file.entries {
        deduped.insert(catalog_entry_to_inventory_entry(entry, resolver)?);
    }

    Ok(deduped.finish())
}

fn read_legacy_segment_catalog_bounded(path: &Path) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog must be a plain regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES as u64 {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "legacy segment catalog bounded decode",
            limit: SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES as u64,
            required: metadata.len(),
        });
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut file = options
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let opened_metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if is_link_or_reparse_point(&opened_metadata)
        || !opened_metadata.file_type().is_file()
        || opened_metadata.len() != metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "segment catalog changed type or length while opening: {}",
            path.display()
        )));
    }
    let initial_capacity = usize::try_from(opened_metadata.len())
        .unwrap_or(SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES)
        .min(SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES);
    crate::engine::binio::read_to_end_bounded(
        &mut file,
        SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES,
        initial_capacity,
        "legacy segment catalog",
    )
}

pub(in crate::engine::storage_engine) fn validate_restore_segment_catalog(
    validation_root: &Path,
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
) -> Result<()> {
    let path = validation_root.join(SEGMENT_CATALOG_FILE_NAME);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(TsinkError::IoWithPath { path, source }),
    }
    let synthetic_tiered_storage = TieredStorageConfig {
        object_store_root: validation_root.join(".restore-validation-object-store"),
        segment_catalog_path: Some(path.clone()),
        mirror_hot_segments: false,
        hot_retention_window: 0,
        warm_retention_window: 0,
    };
    load_segment_catalog(
        &path,
        numeric_lane_path,
        blob_lane_path,
        Some(&synthetic_tiered_storage),
    )
    .map(|_| ())
}

fn catalog_entry_to_inventory_entry(
    entry: SegmentCatalogEntry,
    resolver: SegmentPathResolver<'_>,
) -> Result<SegmentInventoryEntry> {
    let manifest = SegmentManifest {
        segment_id: entry.segment_id,
        level: entry.level,
        chunk_count: entry.chunk_count,
        point_count: entry.point_count,
        series_count: entry.series_count,
        min_ts: entry.min_ts,
        max_ts: entry.max_ts,
        wal_highwater: WalHighWatermark {
            segment: entry.wal_highwater_segment,
            frame: entry.wal_highwater_frame,
        },
    };
    let relative_path = validated_catalog_relative_path(&entry.relative_path, &manifest)?;
    let base_path = resolver.catalog_lane_root(entry.lane, entry.tier)?;
    Ok(SegmentInventoryEntry {
        lane: entry.lane,
        tier: entry.tier,
        root: base_path.join(relative_path),
        manifest,
    })
}

fn validated_catalog_relative_path(
    relative_path: &str,
    manifest: &SegmentManifest,
) -> Result<PathBuf> {
    let path = PathBuf::from(relative_path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::ParentDir
            )
        })
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "segment catalog entry path must stay within the configured lane root: {relative_path}"
        )));
    }

    let expected = relative_segment_path(manifest);
    if path != expected {
        return Err(TsinkError::InvalidConfiguration(format!(
            "segment catalog entry path {relative_path} did not match expected relative path {}",
            expected.display()
        )));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn config(root: &Path) -> TieredStorageConfig {
        TieredStorageConfig {
            object_store_root: root.to_path_buf(),
            segment_catalog_path: None,
            mirror_hot_segments: false,
            hot_retention_window: 10,
            warm_retention_window: 50,
        }
    }

    fn inventory_entry(
        config: &TieredStorageConfig,
        lane: SegmentLaneFamily,
        tier: PersistedSegmentTier,
        level: u8,
        segment_id: u64,
    ) -> SegmentInventoryEntry {
        let manifest = SegmentManifest {
            segment_id,
            level,
            chunk_count: 3,
            point_count: 5,
            series_count: 2,
            min_ts: Some(10),
            max_ts: Some(20),
            wal_highwater: WalHighWatermark {
                segment: 7,
                frame: 11,
            },
        };
        SegmentInventoryEntry {
            lane,
            tier,
            root: config
                .lane_path(lane, tier)
                .join(relative_segment_path(&manifest)),
            manifest,
        }
    }

    fn legacy_preflight_bytes(entry_count: usize) -> Vec<u8> {
        let mut bytes = SEGMENT_CATALOG_LEGACY_STREAM_PREFIX.to_vec();
        for index in 0..entry_count {
            if index != 0 {
                bytes.push(b',');
            }
            bytes.extend_from_slice(b"null");
        }
        bytes.extend_from_slice(SEGMENT_CATALOG_LEGACY_STREAM_SUFFIX);
        bytes
    }

    #[test]
    fn legacy_v2_preflight_accepts_exact_entry_limit_and_rejects_n_plus_one() {
        let exact: SegmentCatalogPreflight =
            serde_json::from_slice(&legacy_preflight_bytes(SEGMENT_CATALOG_MAX_ENTRIES)).unwrap();
        assert_eq!(exact.version, SEGMENT_CATALOG_VERSION);
        assert_eq!(exact.entry_count, SEGMENT_CATALOG_MAX_ENTRIES);

        let error = serde_json::from_slice::<SegmentCatalogPreflight>(&legacy_preflight_bytes(
            SEGMENT_CATALOG_MAX_ENTRIES + 1,
        ))
        .expect_err("one entry over the bound must fail during preflight");
        assert!(error.to_string().contains("segment catalog has more than"));
    }

    #[test]
    fn legacy_v2_bounded_read_accepts_exact_byte_ceiling_and_rejects_n_plus_one_metadata() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join(SEGMENT_CATALOG_FILE_NAME);
        let file = File::create(&path).unwrap();
        file.set_len(SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES as u64)
            .unwrap();
        drop(file);
        assert_eq!(
            read_legacy_segment_catalog_bounded(&path).unwrap().len(),
            SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES
        );

        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES as u64 + 1)
            .unwrap();
        drop(file);
        assert!(matches!(
            read_legacy_segment_catalog_bounded(&path),
            Err(TsinkError::MaintenanceWorkItemTooLarge {
                limit,
                required,
                ..
            }) if limit == SEGMENT_CATALOG_MAX_LEGACY_JSON_BYTES as u64
                && required == limit + 1
        ));
    }

    #[test]
    fn legacy_v2_loader_rejects_missing_required_and_noncanonical_paths() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let path = temp.path().join(SEGMENT_CATALOG_FILE_NAME);
        let base = serde_json::json!({
            "lane": "numeric",
            "tier": "hot",
            "level": 0,
            "segment_id": 1,
            "chunk_count": 1,
            "point_count": 1,
            "series_count": 1,
            "min_ts": 1,
            "max_ts": 1,
            "wal_highwater_segment": 0,
            "wal_highwater_frame": 0
        });
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "version": 2,
                "entries": [base.clone()]
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(load_segment_catalog(&path, None, None, Some(&config)).is_err());

        let mut escaped = base;
        escaped["relative_path"] = serde_json::json!("../../escape");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "version": 2,
                "entries": [escaped]
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            load_segment_catalog(&path, None, None, Some(&config)),
            Err(TsinkError::InvalidConfiguration(message))
                if message.contains("must stay within")
        ));
    }

    #[test]
    fn v3_pointer_is_fixed_length_checksummed_and_namespace_bounded() {
        let pointer = SegmentCatalogPointer {
            generation: 9,
            entry_count: 0,
            generation_file_len: SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64,
            generation_xxh64: 123,
        };
        let bytes = encode_segment_catalog_pointer(pointer).unwrap();
        assert_eq!(bytes.len(), SEGMENT_CATALOG_POINTER_BYTES);
        assert_eq!(decode_segment_catalog_pointer(&bytes).unwrap(), pointer);

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            decode_segment_catalog_pointer(&trailing),
            Err(TsinkError::DataCorruption(_))
        ));

        let mut corrupt = bytes;
        corrupt[8] ^= 1;
        assert!(matches!(
            decode_segment_catalog_pointer(&corrupt),
            Err(TsinkError::DataCorruption(_))
        ));

        let over_limit = SegmentCatalogPointer {
            generation: 10,
            entry_count: SEGMENT_CATALOG_MAX_ENTRIES as u64 + 1,
            generation_file_len: SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64,
            generation_xxh64: 0,
        };
        assert!(matches!(
            validate_segment_catalog_pointer(over_limit),
            Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                limit: SEGMENT_CATALOG_MAX_ENTRIES,
                ..
            })
        ));
    }

    #[test]
    fn v3_generation_reader_enforces_exact_frame_byte_boundary_and_closes_each_page() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let inventory = SegmentInventory::from_entries(vec![inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Hot,
            0,
            42,
        )]);
        let (bytes, pointer) = encode_segment_catalog_generation(&inventory, 7).unwrap();
        let generation_dir = shared_segment_catalog_generation_directory(&config);
        std::fs::create_dir_all(&generation_dir).unwrap();
        let generation_path = shared_segment_catalog_generation_path(&config, 7);
        std::fs::write(&generation_path, &bytes).unwrap();
        let frame_bytes = bytes.len() - SEGMENT_CATALOG_GENERATION_HEADER_BYTES;

        let mut cursor = SegmentCatalogGenerationReadCursor::new(pointer);
        let header_page = read_segment_catalog_generation_page(
            &mut cursor,
            &generation_path,
            None,
            None,
            Some(&config),
            1,
            SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64,
        )
        .unwrap();
        assert!(header_page.entries.is_empty());
        assert!(!header_page.complete);
        assert_eq!(
            header_page.file_bytes_read,
            SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64
        );

        let one_under = read_segment_catalog_generation_page(
            &mut cursor,
            &generation_path,
            None,
            None,
            Some(&config),
            1,
            u64::try_from(frame_bytes - 1).unwrap(),
        );
        assert!(matches!(
            one_under,
            Err(TsinkError::MaintenanceWorkItemTooLarge { required, .. })
                if required == frame_bytes as u64
        ));

        let exact = read_segment_catalog_generation_page(
            &mut cursor,
            &generation_path,
            None,
            None,
            Some(&config),
            1,
            u64::try_from(frame_bytes).unwrap(),
        )
        .unwrap();
        assert_eq!(exact.entries.len(), 1);
        assert_eq!(exact.file_bytes_read, frame_bytes as u64);
        assert!(exact.complete);
        assert_eq!(exact.entries[0].manifest, inventory.entries()[0].manifest);
    }

    #[test]
    fn v3_generation_rejects_frame_corruption_before_returning_an_entry() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let inventory = SegmentInventory::from_entries(vec![inventory_entry(
            &config,
            SegmentLaneFamily::Blob,
            PersistedSegmentTier::Cold,
            2,
            99,
        )]);
        let (mut bytes, pointer) = encode_segment_catalog_generation(&inventory, 8).unwrap();
        let generation_dir = shared_segment_catalog_generation_directory(&config);
        std::fs::create_dir_all(&generation_dir).unwrap();
        let generation_path = shared_segment_catalog_generation_path(&config, 8);
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&generation_path, bytes).unwrap();

        let mut cursor = SegmentCatalogGenerationReadCursor::new(pointer);
        let result = read_segment_catalog_generation_page(
            &mut cursor,
            &generation_path,
            None,
            None,
            Some(&config),
            1,
            pointer.generation_file_len,
        );
        assert!(matches!(result, Err(TsinkError::DataCorruption(_))));
    }

    #[test]
    fn v3_entry_path_limit_is_checked_before_canonical_path_validation() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let entry = inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Warm,
            1,
            5,
        );
        let mut exact_payload = encode_segment_catalog_entry(&entry).unwrap();
        exact_payload.truncate(SEGMENT_CATALOG_ENTRY_FIXED_BYTES);
        exact_payload[68..72]
            .copy_from_slice(&(SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES as u32).to_le_bytes());
        exact_payload.extend(std::iter::repeat_n(
            b'a',
            SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES,
        ));
        assert!(matches!(
            decode_segment_catalog_entry(
                &exact_payload,
                SegmentPathResolver::new(None, None, Some(&config))
            ),
            Err(TsinkError::DataCorruption(_))
        ));

        let mut over_payload = exact_payload;
        over_payload[68..72]
            .copy_from_slice(&(SEGMENT_CATALOG_MAX_RELATIVE_PATH_BYTES as u32 + 1).to_le_bytes());
        over_payload.push(b'a');
        assert!(matches!(
            decode_segment_catalog_entry(
                &over_payload,
                SegmentPathResolver::new(None, None, Some(&config))
            ),
            Err(TsinkError::MaintenanceWorkItemTooLarge {
                limit,
                required,
                ..
            }) if limit == SEGMENT_CATALOG_MAX_FRAME_BYTES as u64
                && required == SEGMENT_CATALOG_MAX_FRAME_BYTES as u64 + 1
        ));
    }

    #[test]
    fn v3_generation_publication_rejects_duplicate_segment_identity() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let first = inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Hot,
            0,
            1,
        );
        let mut duplicate = first.clone();
        duplicate.tier = PersistedSegmentTier::Warm;
        duplicate.root = config
            .lane_path(duplicate.lane, duplicate.tier)
            .join(relative_segment_path(&duplicate.manifest));
        let inventory = SegmentInventory::from_entries(vec![first, duplicate]);
        assert!(matches!(
            encode_segment_catalog_generation(&inventory, 1),
            Err(TsinkError::DataCorruption(_))
        ));
    }

    #[test]
    fn shared_publication_orders_generation_then_legacy_then_pointer() {
        for failed_stage in [
            SegmentCatalogPublishStage::Generation,
            SegmentCatalogPublishStage::LegacyV2,
            SegmentCatalogPublishStage::PointerPrePublication,
            SegmentCatalogPublishStage::Pointer,
        ] {
            let temp = TempDir::new().unwrap();
            let config = config(temp.path());
            let inventory = SegmentInventory::default();
            let mut observed = Vec::new();
            let result = persist_shared_segment_catalog_budgeted_with_stage_hook(
                &config,
                &inventory,
                None,
                |_| Ok(()),
                |stage| {
                    observed.push(stage);
                    if stage == failed_stage {
                        return Err(TsinkError::Other("injected catalog failure".to_string()));
                    }
                    Ok(())
                },
            );
            assert!(result.is_err());
            assert_eq!(
                observed,
                match failed_stage {
                    SegmentCatalogPublishStage::Generation => {
                        vec![SegmentCatalogPublishStage::Generation]
                    }
                    SegmentCatalogPublishStage::LegacyV2 => vec![
                        SegmentCatalogPublishStage::Generation,
                        SegmentCatalogPublishStage::LegacyV2,
                    ],
                    SegmentCatalogPublishStage::PointerPrePublication => vec![
                        SegmentCatalogPublishStage::Generation,
                        SegmentCatalogPublishStage::LegacyV2,
                        SegmentCatalogPublishStage::PointerPrePublication,
                    ],
                    SegmentCatalogPublishStage::Pointer => vec![
                        SegmentCatalogPublishStage::Generation,
                        SegmentCatalogPublishStage::LegacyV2,
                        SegmentCatalogPublishStage::PointerPrePublication,
                        SegmentCatalogPublishStage::Pointer,
                    ],
                }
            );
            assert!(shared_segment_catalog_generation_directory(&config).exists());
            assert_eq!(
                shared_segment_catalog_path(&config).exists(),
                failed_stage != SegmentCatalogPublishStage::Generation
            );
            assert_eq!(
                shared_segment_catalog_pointer_path(&config).exists(),
                failed_stage == SegmentCatalogPublishStage::Pointer
            );
        }
    }

    #[test]
    fn shared_v3_publication_preserves_readable_v2_and_retries_old_generation_gc() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let first_inventory = SegmentInventory::from_entries(vec![inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Hot,
            0,
            1,
        )]);
        let first =
            persist_shared_segment_catalog_budgeted(&config, &first_inventory, None).unwrap();
        let unknown = shared_segment_catalog_generation_directory(&config).join("keep.me");
        std::fs::write(&unknown, b"unknown").unwrap();

        let second_inventory = SegmentInventory::from_entries(vec![inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Warm,
            1,
            2,
        )]);
        let second =
            persist_shared_segment_catalog_budgeted(&config, &second_inventory, None).unwrap();
        assert!(second.generation > first.generation);
        assert!(!shared_segment_catalog_generation_path(&config, first.generation).exists());
        assert!(shared_segment_catalog_generation_path(&config, second.generation).exists());
        assert!(
            unknown.exists(),
            "unknown namespace entries must never be deleted"
        );

        let legacy = load_segment_catalog(
            &shared_segment_catalog_path(&config),
            None,
            None,
            Some(&config),
        )
        .unwrap();
        assert_eq!(legacy.entries().len(), 1);
        assert_eq!(legacy.entries()[0].manifest.segment_id, 2);
        assert_eq!(
            require_shared_segment_catalog_pointer(&config).unwrap(),
            second
        );
    }

    #[test]
    fn shared_publication_releases_disk_reservations_after_overwrite_and_generation_gc() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let first_inventory = SegmentInventory::from_entries(vec![inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Hot,
            0,
            1,
        )]);
        let first =
            persist_shared_segment_catalog_budgeted(&config, &first_inventory, Some(&budget))
                .unwrap();
        assert_eq!(budget.snapshot().active_reservations, 0);
        assert_eq!(budget.snapshot().reserved_bytes, 0);

        let second_inventory = SegmentInventory::from_entries(vec![inventory_entry(
            &config,
            SegmentLaneFamily::Numeric,
            PersistedSegmentTier::Warm,
            1,
            2,
        )]);
        let second =
            persist_shared_segment_catalog_budgeted(&config, &second_inventory, Some(&budget))
                .unwrap();
        assert!(second.generation > first.generation);
        assert!(!shared_segment_catalog_generation_path(&config, first.generation).exists());
        assert!(shared_segment_catalog_generation_path(&config, second.generation).exists());

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
        assert!(snapshot.reconciliations_total > 1);
    }

    #[test]
    fn finite_reader_refuses_missing_v3_pointer_even_when_v2_is_present() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        persist_segment_catalog(
            &shared_segment_catalog_path(&config),
            &SegmentInventory::default(),
        )
        .unwrap();
        assert!(matches!(
            require_shared_segment_catalog_pointer(&config),
            Err(TsinkError::UnsupportedOperation {
                operation: SEGMENT_CATALOG_POINTER_REQUIRED_OPERATION,
                ..
            })
        ));
        assert!(load_segment_catalog(
            &shared_segment_catalog_path(&config),
            None,
            None,
            Some(&config)
        )
        .is_ok());
    }

    #[test]
    fn generation_namespace_accepts_exact_limit_and_rejects_one_over() {
        let temp = TempDir::new().unwrap();
        let directory = temp.path().join(SEGMENT_CATALOG_GENERATION_DIRECTORY_NAME);
        std::fs::create_dir_all(&directory).unwrap();
        for index in 0..MAX_RECOVERY_NAMESPACE_ENTRIES {
            std::fs::File::create(directory.join(format!("unknown-{index:05x}"))).unwrap();
        }
        assert_eq!(
            count_segment_catalog_generation_namespace(&directory).unwrap(),
            MAX_RECOVERY_NAMESPACE_ENTRIES
        );
        std::fs::File::create(directory.join("one-over")).unwrap();
        assert!(matches!(
            count_segment_catalog_generation_namespace(&directory),
            Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                limit: MAX_RECOVERY_NAMESPACE_ENTRIES,
                required,
                ..
            }) if required == MAX_RECOVERY_NAMESPACE_ENTRIES + 1
        ));
    }

    #[cfg(unix)]
    #[test]
    fn v3_reader_and_cleanup_reject_or_preserve_link_like_entries() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let pointer = SegmentCatalogPointer {
            generation: 1,
            entry_count: 0,
            generation_file_len: SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64,
            generation_xxh64: 0,
        };
        let pointer_target = temp.path().join("pointer-target");
        std::fs::write(
            &pointer_target,
            encode_segment_catalog_pointer(pointer).unwrap(),
        )
        .unwrap();
        symlink(
            &pointer_target,
            shared_segment_catalog_pointer_path(&config),
        )
        .unwrap();
        assert!(matches!(
            load_shared_segment_catalog_pointer(&config),
            Err(TsinkError::DataCorruption(_))
        ));

        std::fs::remove_file(shared_segment_catalog_pointer_path(&config)).unwrap();
        let real_directory = temp.path().join("real-generations");
        std::fs::create_dir_all(&real_directory).unwrap();
        let inventory = SegmentInventory::default();
        let (generation_bytes, valid_pointer) =
            encode_segment_catalog_generation(&inventory, 2).unwrap();
        std::fs::write(
            real_directory.join(segment_catalog_generation_file_name(2)),
            generation_bytes,
        )
        .unwrap();
        symlink(
            &real_directory,
            shared_segment_catalog_generation_directory(&config),
        )
        .unwrap();
        let mut cursor = SegmentCatalogGenerationReadCursor::new(valid_pointer);
        assert!(matches!(
            read_segment_catalog_generation_page(
                &mut cursor,
                &shared_segment_catalog_generation_path(&config, 2),
                None,
                None,
                Some(&config),
                1,
                SEGMENT_CATALOG_GENERATION_HEADER_BYTES as u64,
            ),
            Err(TsinkError::DataCorruption(_))
        ));

        std::fs::remove_file(shared_segment_catalog_generation_directory(&config)).unwrap();
        let generation_directory = shared_segment_catalog_generation_directory(&config);
        std::fs::create_dir_all(&generation_directory).unwrap();
        let outside = temp.path().join("outside-generation");
        std::fs::write(&outside, b"outside").unwrap();
        let link = generation_directory.join(segment_catalog_generation_file_name(3));
        symlink(&outside, &link).unwrap();
        cleanup_old_segment_catalog_generations(&generation_directory, None, None, usize::MAX)
            .unwrap();
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink(),
            "cleanup must leave a canonical-looking symlink untouched",
        );
        assert_eq!(std::fs::read(outside).unwrap(), b"outside");
    }
}
