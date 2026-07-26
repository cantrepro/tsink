use super::*;
use crate::engine::binio::{checksum32, read_to_end_bounded};
use crate::engine::fs_utils::{
    is_link_or_reparse_point, path_exists_no_follow,
    write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit,
};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;

pub(crate) const DATA_DIRECTORY_MANIFEST_FILE_NAME: &str = "tsink-manifest.json";

const DATA_DIRECTORY_MAGIC: &str = "TSINK_DATA_DIRECTORY";
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const MINIMUM_READER_STORAGE_FORMAT_VERSION: u16 = crate::engine::STORAGE_FORMAT_VERSION;
pub(super) const MAX_MANIFEST_FILE_BYTES: usize = 16 * 1024;
const MANIFEST_MEMORY_FIXED_ALLOWANCE_BYTES: usize = 4 * 1024;
const MANIFEST_MEMORY_FILE_COPIES: usize = 4;
const MAX_LEGACY_ROOT_ENTRIES: usize = 128;
const CURRENT_FORMAT_FEATURES: [&str; 4] = [
    "blob_value_lane_v1",
    "framed_segment_v2",
    "framed_wal_v2",
    "registry_snapshot_v2",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestTimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl From<TimestampPrecision> for ManifestTimestampPrecision {
    fn from(value: TimestampPrecision) -> Self {
        match value {
            TimestampPrecision::Nanoseconds => Self::Nanoseconds,
            TimestampPrecision::Microseconds => Self::Microseconds,
            TimestampPrecision::Milliseconds => Self::Milliseconds,
            TimestampPrecision::Seconds => Self::Seconds,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataDirectoryManifestPayload {
    storage_format_version: u16,
    minimum_reader_storage_format_version: u16,
    creating_tsink_version: Option<String>,
    last_successfully_opened_tsink_version: Option<String>,
    format_affecting_features: Vec<String>,
    timestamp_precision: ManifestTimestampPrecision,
    chunk_point_capacity: u32,
    partition_window_timestamp_units: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataDirectoryManifestEnvelope {
    magic: String,
    manifest_schema_version: u16,
    payload_crc32: u32,
    payload: DataDirectoryManifestPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SnapshotValidationConfiguration {
    pub(super) timestamp_precision: TimestampPrecision,
    pub(super) chunk_point_capacity: usize,
    pub(super) partition_duration: Duration,
}

#[derive(Debug, Deserialize)]
struct ManifestHeaderProbe {
    magic: String,
    manifest_schema_version: u16,
    payload: ManifestPayloadVersionProbe,
}

#[derive(Debug, Deserialize)]
struct ManifestPayloadVersionProbe {
    storage_format_version: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingManifestDirectoryKind {
    Empty,
    Legacy,
}

#[derive(Debug, Clone)]
pub(super) struct OpenedDataDirectoryManifest {
    path: PathBuf,
    payload: DataDirectoryManifestPayload,
    startup_memory_budget: usize,
}

pub(super) fn preflight_before_process_lock(builder: &StorageBuilder) -> Result<()> {
    let Some(data_path) = manifest_data_path(builder) else {
        return Ok(());
    };
    inspect_directory(builder, data_path).map(|_| ())
}

pub(super) fn prepare_before_recovery(
    builder: &StorageBuilder,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<Option<OpenedDataDirectoryManifest>> {
    let Some(data_path) = manifest_data_path(builder) else {
        return Ok(None);
    };
    let expected = expected_payload(builder, None, Some(env!("CARGO_PKG_VERSION")))?;
    let path = data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
    let payload = match inspect_directory(builder, data_path)? {
        DirectoryInspection::Current(payload) => {
            validate_immutable_configuration(&payload, &expected, &path)?;
            payload
        }
        DirectoryInspection::Missing(kind) => {
            let creating_version = match kind {
                MissingManifestDirectoryKind::Empty => Some(env!("CARGO_PKG_VERSION").to_string()),
                MissingManifestDirectoryKind::Legacy => None,
            };
            let payload = expected_payload(builder, creating_version, None)?;
            if path_exists_no_follow(&path)? {
                return Err(TsinkError::DataCorruption(format!(
                    "data-directory manifest appeared after locked validation: {}",
                    path.display()
                )));
            }
            persist_manifest(
                &path,
                &payload,
                local_disk_budget,
                builder.memory_limit_bytes(),
            )?;
            payload
        }
    };

    Ok(Some(OpenedDataDirectoryManifest {
        path,
        payload,
        startup_memory_budget: builder.memory_limit_bytes(),
    }))
}

impl OpenedDataDirectoryManifest {
    pub(super) fn record_successful_open(
        &self,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        let current = read_manifest(&self.path, self.startup_memory_budget)?;
        if current != self.payload {
            return Err(TsinkError::DataCorruption(format!(
                "data-directory manifest changed during startup: {}",
                self.path.display()
            )));
        }
        if current.last_successfully_opened_tsink_version.as_deref()
            == Some(env!("CARGO_PKG_VERSION"))
        {
            return Ok(());
        }

        let mut updated = current;
        updated.last_successfully_opened_tsink_version =
            Some(env!("CARGO_PKG_VERSION").to_string());
        persist_manifest(
            &self.path,
            &updated,
            local_disk_budget,
            self.startup_memory_budget,
        )
    }
}

pub(super) fn validate_snapshot_manifest_bytes(
    bytes: &[u8],
    source: &Path,
) -> Result<SnapshotValidationConfiguration> {
    let payload = decode_manifest(bytes, source)?;
    if payload.storage_format_version > crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot data-directory storage format {} is newer than the supported format {}: {}",
            payload.storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            source.display()
        )));
    }
    if payload.storage_format_version < crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot data-directory storage format {} is older than the supported format {}; no in-place migration is available: {}",
            payload.storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            source.display()
        )));
    }
    if payload.minimum_reader_storage_format_version > crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot data-directory minimum reader format {} exceeds supported format {}: {}",
            payload.minimum_reader_storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            source.display()
        )));
    }
    if payload.minimum_reader_storage_format_version > payload.storage_format_version {
        return Err(TsinkError::DataCorruption(format!(
            "snapshot data-directory minimum reader format {} exceeds its storage format {}: {}",
            payload.minimum_reader_storage_format_version,
            payload.storage_format_version,
            source.display()
        )));
    }
    let timestamp_precision = match payload.timestamp_precision {
        ManifestTimestampPrecision::Nanoseconds => TimestampPrecision::Nanoseconds,
        ManifestTimestampPrecision::Microseconds => TimestampPrecision::Microseconds,
        ManifestTimestampPrecision::Milliseconds => TimestampPrecision::Milliseconds,
        ManifestTimestampPrecision::Seconds => TimestampPrecision::Seconds,
    };
    let chunk_point_capacity = usize::try_from(payload.chunk_point_capacity).map_err(|_| {
        TsinkError::InvalidConfiguration(format!(
            "snapshot chunk-point capacity {} exceeds this platform's range: {}",
            payload.chunk_point_capacity,
            source.display()
        ))
    })?;
    if !(1..=u16::MAX as usize).contains(&chunk_point_capacity) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot chunk-point capacity {} is outside the supported range 1..={}: {}",
            payload.chunk_point_capacity,
            u16::MAX,
            source.display()
        )));
    }
    let partition_units =
        u64::try_from(payload.partition_window_timestamp_units).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "snapshot partition window must be positive, got {}: {}",
                payload.partition_window_timestamp_units,
                source.display()
            ))
        })?;
    if partition_units == 0 {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot partition window must be positive: {}",
            source.display()
        )));
    }
    let partition_duration = match timestamp_precision {
        TimestampPrecision::Nanoseconds => Duration::from_nanos(partition_units),
        TimestampPrecision::Microseconds => Duration::from_micros(partition_units),
        TimestampPrecision::Milliseconds => Duration::from_millis(partition_units),
        TimestampPrecision::Seconds => Duration::from_secs(partition_units),
    };
    Ok(SnapshotValidationConfiguration {
        timestamp_precision,
        chunk_point_capacity,
        partition_duration,
    })
}

#[cfg(test)]
pub(super) fn install_current_manifest_for_test(builder: &StorageBuilder) -> Result<u64> {
    let data_path = manifest_data_path(builder).ok_or_else(|| {
        TsinkError::InvalidConfiguration(
            "test manifest installation requires a read-write data path".to_string(),
        )
    })?;
    std::fs::create_dir_all(data_path)?;
    let payload = expected_payload(
        builder,
        Some(env!("CARGO_PKG_VERSION").to_string()),
        Some(env!("CARGO_PKG_VERSION")),
    )?;
    let bytes = encode_manifest(&payload)?;
    let encoded_len = u64::try_from(bytes.len())
        .map_err(|_| TsinkError::Other("test manifest length exceeds u64".to_string()))?;
    crate::engine::fs_utils::write_file_atomically_and_sync_parent(
        &data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME),
        &bytes,
    )?;
    Ok(encoded_len)
}

enum DirectoryInspection {
    Current(DataDirectoryManifestPayload),
    Missing(MissingManifestDirectoryKind),
}

fn manifest_data_path(builder: &StorageBuilder) -> Option<&Path> {
    (builder.runtime_mode() == StorageRuntimeMode::ReadWrite)
        .then(|| builder.data_path())
        .flatten()
}

fn inspect_directory(builder: &StorageBuilder, data_path: &Path) -> Result<DirectoryInspection> {
    let root_metadata = match std::fs::symlink_metadata(data_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DirectoryInspection::Missing(
                MissingManifestDirectoryKind::Empty,
            ))
        }
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: data_path.to_path_buf(),
                source,
            })
        }
    };
    if !root_metadata.file_type().is_dir() || is_link_or_reparse_point(&root_metadata) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data path must be a real directory before manifest validation: {}",
            data_path.display()
        )));
    }

    let manifest_path = data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
    match std::fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.file_type().is_file() && !is_link_or_reparse_point(&metadata) => {
            let payload = read_manifest(&manifest_path, builder.memory_limit_bytes())?;
            let expected = expected_payload(builder, None, Some(env!("CARGO_PKG_VERSION")))?;
            validate_immutable_configuration(&payload, &expected, &manifest_path)?;
            return Ok(DirectoryInspection::Current(payload));
        }
        Ok(_) => {
            return Err(TsinkError::DataCorruption(format!(
                "data-directory manifest must be a regular non-link file: {}",
                manifest_path.display()
            )))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: manifest_path,
                source,
            })
        }
    }

    inspect_missing_manifest_namespace(data_path, builder.memory_limit_bytes())
        .map(DirectoryInspection::Missing)
}

fn inspect_missing_manifest_namespace(
    data_path: &Path,
    startup_memory_budget: usize,
) -> Result<MissingManifestDirectoryKind> {
    let mut observed_entries = 0usize;
    let mut observed_legacy_state = false;
    for entry in std::fs::read_dir(data_path).map_err(|source| TsinkError::IoWithPath {
        path: data_path.to_path_buf(),
        source,
    })? {
        observed_entries = observed_entries.checked_add(1).ok_or_else(|| {
            TsinkError::Other("legacy data-directory entry counter overflow".to_string())
        })?;
        if observed_entries > MAX_LEGACY_ROOT_ENTRIES {
            return Err(TsinkError::DataCorruption(format!(
                "legacy data-directory root exceeds its {MAX_LEGACY_ROOT_ENTRIES}-entry validation bound: {}",
                data_path.display()
            )));
        }
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: data_path.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(unknown_legacy_entry_error(&entry.path()));
        };
        let expected_kind = legacy_root_entry_kind(name)
            .ok_or_else(|| unknown_legacy_entry_error(&entry.path()))?;
        validate_legacy_entry_type(name, &entry.path(), expected_kind)?;
        if name != ".tsink.lock" && !is_manifest_atomic_temp(name) {
            observed_legacy_state = true;
        }
    }

    if observed_legacy_state {
        validate_legacy_core_identity(data_path, startup_memory_budget)?;
        Ok(MissingManifestDirectoryKind::Legacy)
    } else {
        Ok(MissingManifestDirectoryKind::Empty)
    }
}

fn validate_legacy_core_identity(data_path: &Path, startup_memory_budget: usize) -> Result<()> {
    let snapshot_path = data_path.join(SERIES_INDEX_FILE_NAME);
    let mut admitted_bytes = 0usize;
    let loaded = SeriesRegistry::load_persisted_state_with_startup_admission(
        &snapshot_path,
        startup_memory_budget,
        |additional_bytes| {
            let required = admitted_bytes
                .checked_add(additional_bytes)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "legacy registry identity memory accounting overflow".to_string(),
                    )
                })?;
            if startup_memory_budget != usize::MAX && required > startup_memory_budget {
                return Err(TsinkError::MemoryBudgetExceeded {
                    budget: startup_memory_budget,
                    required,
                });
            }
            admitted_bytes = required;
            Ok(())
        },
    )
    .map_err(|err| match err {
        err @ TsinkError::MemoryBudgetExceeded { .. } => err,
        err => TsinkError::DataCorruption(format!(
            "manifestless legacy directory failed read-only series-registry identity validation at {}: {err}",
            snapshot_path.display()
        )),
    })?;
    if loaded.is_some() {
        return Ok(());
    }

    let wal_path = data_path.join(WAL_DIR_NAME);
    let valid_wal =
        crate::engine::wal::validate_legacy_wal_identity(&wal_path, startup_memory_budget)
            .map_err(|err| match err {
                err @ TsinkError::MemoryBudgetExceeded { .. } => err,
                err => TsinkError::DataCorruption(format!(
                    "manifestless legacy directory failed read-only framed-WAL identity validation at {}: {err}",
                    wal_path.display()
                )),
            })?;
    if valid_wal {
        return Ok(());
    }

    let valid_segment = crate::engine::segment::validate_legacy_segment_identity(
        data_path,
        startup_memory_budget,
    )
    .map_err(|err| match err {
        err @ TsinkError::MemoryBudgetExceeded { .. } => err,
        err => TsinkError::DataCorruption(format!(
            "manifestless legacy directory failed read-only persisted-segment identity validation under {}: {err}",
            data_path.display()
        )),
    })?;
    if valid_segment {
        return Ok(());
    }

    Err(TsinkError::InvalidConfiguration(format!(
        "missing {DATA_DIRECTORY_MANIFEST_FILE_NAME}; the exact legacy namespace has no valid series-registry identity at {}, framed-WAL identity at {}, or rebuildable v2 persisted-segment identity, so inspection or explicit recovery is required",
        snapshot_path.display(),
        wal_path.display()
    )))
}

#[derive(Debug, Clone, Copy)]
enum LegacyRootEntryKind {
    File,
    Directory,
}

fn legacy_root_entry_kind(name: &str) -> Option<LegacyRootEntryKind> {
    use LegacyRootEntryKind::{Directory, File};

    match name {
        ".tsink.lock"
        | "series_index.bin"
        | "series_index.delta.bin"
        | "series_index.catalog.json"
        | "segment_catalog.json"
        | "metric-metadata-store.json"
        | "exemplar-store.json"
        | "rules-store.json" => Some(File),
        "lane_numeric"
        | "lane_blob"
        | "wal"
        | "series_index.delta.d"
        | "series_index.catalog.d"
        | ".tombstone-transactions"
        | ".post-flush-replacements"
        | ".rollups"
        | "usage-accounting"
        | "managed-control-plane"
        | "edge_sync"
        | "cluster" => Some(Directory),
        _ if legacy_atomic_temp_target(name).is_some_and(is_known_legacy_root_file) => Some(File),
        _ if is_exact_post_flush_rewrite_staging_name(name) => Some(Directory),
        _ => None,
    }
}

fn is_known_legacy_root_file(name: &str) -> bool {
    matches!(
        name,
        "series_index.bin"
            | "series_index.delta.bin"
            | "series_index.catalog.json"
            | "segment_catalog.json"
            | DATA_DIRECTORY_MANIFEST_FILE_NAME
            | "metric-metadata-store.json"
            | "exemplar-store.json"
            | "rules-store.json"
    )
}

fn is_manifest_atomic_temp(name: &str) -> bool {
    legacy_atomic_temp_target(name) == Some(DATA_DIRECTORY_MANIFEST_FILE_NAME)
}

fn legacy_atomic_temp_target(name: &str) -> Option<&str> {
    let generated = name.strip_prefix('.')?;
    let (target, suffix) = generated.rsplit_once(".tmp-")?;
    let (pid, nonce) = suffix.split_once('-')?;
    let canonical_pid = pid
        .parse::<u32>()
        .ok()
        .is_some_and(|value| value.to_string() == pid);
    (canonical_pid && is_exact_lower_hex(nonce, 16)).then_some(target)
}

fn is_exact_post_flush_rewrite_staging_name(name: &str) -> bool {
    ["lane_numeric", "lane_blob"].into_iter().any(|lane| {
        name.strip_prefix(&format!(".tmp-tsink-post-flush-retention-rewrite-{lane}-"))
            .is_some_and(|nonce| is_exact_lower_hex(nonce, 16))
    })
}

fn is_exact_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_legacy_entry_type(
    name: &str,
    path: &Path,
    expected: LegacyRootEntryKind,
) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if is_link_or_reparse_point(&metadata) {
        let description = if name == ".tsink.lock" {
            "data path lock must be a regular file"
        } else {
            match expected {
                LegacyRootEntryKind::File => "legacy managed file must be a regular file",
                LegacyRootEntryKind::Directory => {
                    "legacy managed directory must be a real directory"
                }
            }
        };
        return Err(TsinkError::InvalidConfiguration(format!(
            "{description}: {}",
            path.display()
        )));
    }
    let matches = match expected {
        LegacyRootEntryKind::File => metadata.file_type().is_file(),
        LegacyRootEntryKind::Directory => metadata.file_type().is_dir(),
    };
    if !matches {
        return Err(TsinkError::InvalidConfiguration(format!(
            "legacy managed {} has the wrong entry type: {}",
            match expected {
                LegacyRootEntryKind::File => "file",
                LegacyRootEntryKind::Directory => "directory",
            },
            path.display()
        )));
    }
    Ok(())
}

fn unknown_legacy_entry_error(path: &Path) -> TsinkError {
    TsinkError::InvalidConfiguration(format!(
        "missing {DATA_DIRECTORY_MANIFEST_FILE_NAME}; refusing to guess the format of a directory containing unknown legacy entry {}",
        path.display()
    ))
}

fn expected_payload(
    builder: &StorageBuilder,
    creating_tsink_version: Option<String>,
    last_successfully_opened_tsink_version: Option<&str>,
) -> Result<DataDirectoryManifestPayload> {
    let partition_window_timestamp_units =
        duration_to_timestamp_units(builder.partition_duration(), builder.timestamp_precision())
            .max(1);
    Ok(DataDirectoryManifestPayload {
        storage_format_version: crate::engine::STORAGE_FORMAT_VERSION,
        minimum_reader_storage_format_version: MINIMUM_READER_STORAGE_FORMAT_VERSION,
        creating_tsink_version,
        last_successfully_opened_tsink_version: last_successfully_opened_tsink_version
            .map(str::to_string),
        format_affecting_features: CURRENT_FORMAT_FEATURES
            .into_iter()
            .map(str::to_string)
            .collect(),
        timestamp_precision: builder.timestamp_precision().into(),
        chunk_point_capacity: u32::try_from(builder.chunk_points()).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "chunk point capacity {} exceeds manifest range",
                builder.chunk_points()
            ))
        })?,
        partition_window_timestamp_units,
    })
}

fn validate_immutable_configuration(
    actual: &DataDirectoryManifestPayload,
    expected: &DataDirectoryManifestPayload,
    path: &Path,
) -> Result<()> {
    if actual.storage_format_version > crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data-directory storage format {} is newer than supported format {}: {}",
            actual.storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            path.display()
        )));
    }
    if actual.storage_format_version < crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data-directory storage format {} is older than the supported format {}; no in-place migration is available: {}",
            actual.storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            path.display()
        )));
    }
    if actual.minimum_reader_storage_format_version > crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data-directory minimum reader format {} exceeds supported format {}: {}",
            actual.minimum_reader_storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            path.display()
        )));
    }
    if actual.minimum_reader_storage_format_version > actual.storage_format_version {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory minimum reader format {} exceeds its storage format {}: {}",
            actual.minimum_reader_storage_format_version,
            actual.storage_format_version,
            path.display()
        )));
    }

    let mut mismatches = Vec::new();
    if actual.format_affecting_features != expected.format_affecting_features {
        mismatches.push("format_affecting_features");
    }
    if actual.timestamp_precision != expected.timestamp_precision {
        mismatches.push("timestamp_precision");
    }
    if actual.chunk_point_capacity != expected.chunk_point_capacity {
        mismatches.push("chunk_point_capacity");
    }
    if actual.partition_window_timestamp_units != expected.partition_window_timestamp_units {
        mismatches.push("partition_window_timestamp_units");
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::InvalidConfiguration(format!(
            "data-directory immutable configuration mismatch ({}) in {}; reopen with the creating timestamp precision, chunk capacity, and partition window",
            mismatches.join(", "),
            path.display()
        )))
    }
}

fn read_manifest(path: &Path, memory_budget: usize) -> Result<DataDirectoryManifestPayload> {
    let bytes = read_manifest_bytes(path, memory_budget)?;
    decode_manifest(&bytes, path)
}

fn read_manifest_bytes(path: &Path, memory_budget: usize) -> Result<Vec<u8>> {
    let path_metadata =
        std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if !path_metadata.file_type().is_file() || is_link_or_reparse_point(&path_metadata) {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest must be a regular non-link file: {}",
            path.display()
        )));
    }
    let file_len = usize::try_from(path_metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "data-directory manifest length exceeds this platform's range: {}",
            path.display()
        ))
    })?;
    if file_len > MAX_MANIFEST_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest exceeds its {MAX_MANIFEST_FILE_BYTES}-byte format limit: {}",
            path.display()
        )));
    }
    admit_manifest_memory(memory_budget, file_len)?;

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
    if !opened_metadata.file_type().is_file()
        || is_link_or_reparse_point(&opened_metadata)
        || opened_metadata.len() != path_metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest changed type or length while opening: {}",
            path.display()
        )));
    }
    let bytes = read_to_end_bounded(
        &mut file,
        MAX_MANIFEST_FILE_BYTES,
        file_len,
        "data-directory manifest",
    )?;
    if bytes.len() != file_len {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest length changed while reading: {}",
            path.display()
        )));
    }
    let opened_identity =
        same_file::Handle::from_file(file).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let current_metadata =
        std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if !current_metadata.file_type().is_file()
        || is_link_or_reparse_point(&current_metadata)
        || current_metadata.len() != path_metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest path changed type or length while reading: {}",
            path.display()
        )));
    }
    let current_identity =
        same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if opened_identity != current_identity {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest path changed while reading: {}",
            path.display()
        )));
    }
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8], path: &Path) -> Result<DataDirectoryManifestPayload> {
    let probe: ManifestHeaderProbe = serde_json::from_slice(bytes).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "data-directory manifest JSON is corrupt at {}: {err}",
            path.display()
        ))
    })?;
    if probe.magic != DATA_DIRECTORY_MAGIC {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest magic is invalid: {}",
            path.display()
        )));
    }
    if probe.manifest_schema_version > MANIFEST_SCHEMA_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data-directory manifest schema {} is newer than supported schema {MANIFEST_SCHEMA_VERSION}: {}",
            probe.manifest_schema_version,
            path.display()
        )));
    }
    if probe.manifest_schema_version < MANIFEST_SCHEMA_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data-directory manifest schema {} is older than supported schema {MANIFEST_SCHEMA_VERSION}: {}",
            probe.manifest_schema_version,
            path.display()
        )));
    }
    if probe.payload.storage_format_version > crate::engine::STORAGE_FORMAT_VERSION {
        return Err(TsinkError::InvalidConfiguration(format!(
            "data-directory storage format {} is newer than supported format {}: {}",
            probe.payload.storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION,
            path.display()
        )));
    }

    let envelope: DataDirectoryManifestEnvelope = serde_json::from_slice(bytes).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "data-directory manifest payload is corrupt at {}: {err}",
            path.display()
        ))
    })?;
    let encoded_payload = serde_json::to_vec(&envelope.payload).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "data-directory manifest payload cannot be canonicalized at {}: {err}",
            path.display()
        ))
    })?;
    let actual_crc32 = checksum32(&encoded_payload);
    if actual_crc32 != envelope.payload_crc32 {
        return Err(TsinkError::DataCorruption(format!(
            "data-directory manifest checksum mismatch: expected {:08x}, got {actual_crc32:08x}: {}",
            envelope.payload_crc32,
            path.display()
        )));
    }
    Ok(envelope.payload)
}

fn persist_manifest(
    path: &Path,
    payload: &DataDirectoryManifestPayload,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget: usize,
) -> Result<()> {
    admit_manifest_memory(memory_budget, MAX_MANIFEST_FILE_BYTES)?;
    let bytes = encode_manifest(payload)?;
    write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
        path,
        bytes,
        local_disk_budget,
        crate::DiskCategory::Registry,
        crate::disk_budget::DiskReservationKind::Recovery,
        memory_budget,
    )
}

fn encode_manifest(payload: &DataDirectoryManifestPayload) -> Result<Vec<u8>> {
    let encoded_payload = serde_json::to_vec(payload)?;
    let envelope = DataDirectoryManifestEnvelope {
        magic: DATA_DIRECTORY_MAGIC.to_string(),
        manifest_schema_version: MANIFEST_SCHEMA_VERSION,
        payload_crc32: checksum32(&encoded_payload),
        payload: payload.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&envelope)?;
    if bytes.len() > MAX_MANIFEST_FILE_BYTES {
        return Err(TsinkError::Other(format!(
            "encoded data-directory manifest exceeds its {MAX_MANIFEST_FILE_BYTES}-byte limit"
        )));
    }
    Ok(bytes)
}

fn admit_manifest_memory(memory_budget: usize, file_bytes: usize) -> Result<()> {
    let required = file_bytes
        .checked_mul(MANIFEST_MEMORY_FILE_COPIES)
        .and_then(|bytes| bytes.checked_add(MANIFEST_MEMORY_FIXED_ALLOWANCE_BYTES))
        .ok_or_else(|| {
            TsinkError::Other("data-directory manifest memory model overflow".to_string())
        })?;
    crate::disk_budget::admit_startup_memory(memory_budget, required)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::chunk::{ChunkPoint, ValueLane};
    use crate::engine::wal::{FramedWal, SamplesBatchFrame, SeriesDefinitionFrame};
    use crate::{DataPoint, ResourceProfile, Row, Value, WalSyncMode};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn builder(path: &Path) -> StorageBuilder {
        StorageBuilder::new()
            .with_resource_profile(ResourceProfile::ExpertUnlimited)
            .with_data_path(path)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .with_partition_duration(Duration::from_secs(60))
    }

    fn manifest_path(path: &Path) -> PathBuf {
        path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME)
    }

    fn directory_bytes(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn visit(root: &Path, current: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
            let mut entries = std::fs::read_dir(current)
                .unwrap()
                .map(|entry| entry.unwrap())
                .collect::<Vec<_>>();
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let path = entry.path();
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                let metadata = std::fs::symlink_metadata(&path).unwrap();
                if metadata.file_type().is_dir() {
                    out.insert(relative.clone(), b"<directory>".to_vec());
                    visit(root, &path, out);
                } else if metadata.file_type().is_file() {
                    out.insert(relative, std::fs::read(path).unwrap());
                } else {
                    out.insert(relative, b"<other>".to_vec());
                }
            }
        }

        let mut out = BTreeMap::new();
        visit(path, path, &mut out);
        out
    }

    fn rewrite_manifest(path: &Path, mutate: impl FnOnce(&mut DataDirectoryManifestPayload)) {
        let mut payload = read_manifest(path, usize::MAX).unwrap();
        mutate(&mut payload);
        std::fs::write(path, encode_manifest(&payload).unwrap()).unwrap();
    }

    fn remove_registry_identity(path: &Path) {
        for relative in [
            "series_index.bin",
            "series_index.delta.bin",
            "series_index.catalog.json",
        ] {
            let candidate = path.join(relative);
            if candidate.exists() {
                std::fs::remove_file(candidate).unwrap();
            }
        }
        for relative in ["series_index.delta.d", "series_index.catalog.d"] {
            let candidate = path.join(relative);
            if candidate.exists() {
                std::fs::remove_dir_all(candidate).unwrap();
            }
        }
    }

    fn build_error(builder: StorageBuilder) -> TsinkError {
        match builder.build() {
            Ok(storage) => {
                storage.close().unwrap();
                panic!("expected storage build to fail");
            }
            Err(err) => err,
        }
    }

    #[test]
    fn empty_directory_is_initialized_atomically_and_records_successful_open() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");

        let storage = builder(&data_path).build().unwrap();
        storage.close().unwrap();

        let payload = read_manifest(&manifest_path(&data_path), usize::MAX).unwrap();
        assert_eq!(
            payload.creating_tsink_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            payload.last_successfully_opened_tsink_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            payload.storage_format_version,
            crate::engine::STORAGE_FORMAT_VERSION
        );
        assert!(!std::fs::read_dir(&data_path).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tsink-manifest.json.tmp-")
        }));
    }

    #[test]
    fn exact_legacy_namespace_migrates_and_preserves_data() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        {
            let storage = builder(&data_path).build().unwrap();
            storage
                .insert_rows(&[Row::new("legacy_metric", DataPoint::new(1, 7.0))])
                .unwrap();
            storage.close().unwrap();
        }
        std::fs::remove_file(manifest_path(&data_path)).unwrap();

        let reopened = builder(&data_path).build().unwrap();
        assert_eq!(
            reopened.select("legacy_metric", &[], 0, 2).unwrap(),
            vec![DataPoint::new(1, 7.0)]
        );
        reopened.close().unwrap();

        let payload = read_manifest(&manifest_path(&data_path), usize::MAX).unwrap();
        assert_eq!(payload.creating_tsink_version, None);
        assert_eq!(
            payload.last_successfully_opened_tsink_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn valid_framed_wal_only_legacy_namespace_is_migrated() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let wal = FramedWal::open(data_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "wal_only_legacy".to_string(),
            labels: Vec::new(),
        })
        .unwrap();
        wal.append_samples(&[SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 1,
                value: Value::F64(7.0),
            }],
        )
        .unwrap()])
            .unwrap();
        drop(wal);

        let reopened = builder(&data_path).build().unwrap();
        assert_eq!(
            reopened.select("wal_only_legacy", &[], 0, 2).unwrap(),
            vec![DataPoint::new(1, 7.0)]
        );
        reopened.close().unwrap();

        let payload = read_manifest(&manifest_path(&data_path), usize::MAX).unwrap();
        assert_eq!(payload.creating_tsink_version, None);
        assert_eq!(
            payload.last_successfully_opened_tsink_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn exact_empty_wal_bootstrap_namespace_is_migrated() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let wal = FramedWal::open(data_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        drop(wal);

        let reopened = builder(&data_path).build().unwrap();
        reopened.close().unwrap();
        assert!(manifest_path(&data_path).is_file());
    }

    #[test]
    fn corrupt_known_wal_segment_is_rejected_without_mutation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let wal_path = data_path
            .join(WAL_DIR_NAME)
            .join("wal-0000000000000000.log");
        std::fs::create_dir_all(wal_path.parent().unwrap()).unwrap();
        std::fs::write(&wal_path, b"foreign bytes under a managed WAL name").unwrap();
        let before = directory_bytes(&data_path);

        let err = build_error(builder(&data_path));
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("read-only framed-WAL identity validation")));
        assert_eq!(directory_bytes(&data_path), before);
        assert!(!data_path.join(".tsink.lock").exists());
        assert!(!manifest_path(&data_path).exists());
    }

    #[test]
    fn valid_segment_only_legacy_namespace_rebuilds_registry_and_migrates() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        {
            let storage = builder(&data_path).with_wal_enabled(false).build().unwrap();
            storage
                .insert_rows(&[
                    Row::new("segment_only_legacy", DataPoint::new(1, 4.0)),
                    Row::new("segment_only_legacy", DataPoint::new(2, 5.0)),
                ])
                .unwrap();
            storage.close().unwrap();
        }
        std::fs::remove_file(manifest_path(&data_path)).unwrap();
        remove_registry_identity(&data_path);
        assert!(
            !crate::engine::segment::list_segment_dirs(data_path.join(NUMERIC_LANE_ROOT))
                .unwrap()
                .is_empty()
        );

        let reopened = builder(&data_path).with_wal_enabled(false).build().unwrap();
        assert_eq!(
            reopened.select("segment_only_legacy", &[], 0, 3).unwrap(),
            vec![DataPoint::new(1, 4.0), DataPoint::new(2, 5.0)]
        );
        reopened.close().unwrap();
        assert_eq!(
            read_manifest(&manifest_path(&data_path), usize::MAX)
                .unwrap()
                .creating_tsink_version,
            None
        );
    }

    #[test]
    fn corrupt_segment_only_legacy_identity_is_rejected_without_mutation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        {
            let storage = builder(&data_path).with_wal_enabled(false).build().unwrap();
            storage
                .insert_rows(&[
                    Row::new("corrupt_segment_legacy", DataPoint::new(1, 1.0)),
                    Row::new("corrupt_segment_legacy", DataPoint::new(2, 2.0)),
                ])
                .unwrap();
            storage.close().unwrap();
        }
        std::fs::remove_file(manifest_path(&data_path)).unwrap();
        remove_registry_identity(&data_path);
        let lock_path = data_path.join(".tsink.lock");
        if lock_path.exists() {
            std::fs::remove_file(&lock_path).unwrap();
        }
        let segment_root =
            crate::engine::segment::list_segment_dirs(data_path.join(NUMERIC_LANE_ROOT))
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
        std::fs::write(segment_root.join("manifest.bin"), b"foreign segment bytes").unwrap();
        let before = directory_bytes(&data_path);

        let err = build_error(builder(&data_path).with_wal_enabled(false));
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("read-only persisted-segment identity validation")));
        assert_eq!(directory_bytes(&data_path), before);
        assert!(!data_path.join(".tsink.lock").exists());
        assert!(!manifest_path(&data_path).exists());
    }

    #[test]
    fn corrupt_manifest_is_rejected_without_directory_mutation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let storage = builder(&data_path).build().unwrap();
        storage.close().unwrap();
        let path = manifest_path(&data_path);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["payload"]["chunk_point_capacity"] = serde_json::json!(99);
        std::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        let before = directory_bytes(&data_path);

        let err = build_error(builder(&data_path));
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("manifest checksum mismatch")));
        assert_eq!(directory_bytes(&data_path), before);
    }

    #[test]
    fn newer_manifest_is_rejected_without_directory_mutation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let storage = builder(&data_path).build().unwrap();
        storage.close().unwrap();
        rewrite_manifest(&manifest_path(&data_path), |payload| {
            payload.storage_format_version =
                crate::engine::STORAGE_FORMAT_VERSION.saturating_add(1);
        });
        let before = directory_bytes(&data_path);

        let err = build_error(builder(&data_path));
        assert!(matches!(err, TsinkError::InvalidConfiguration(message)
            if message.contains("newer than supported format")));
        assert_eq!(directory_bytes(&data_path), before);
    }

    #[test]
    fn unknown_manifestless_directory_is_rejected_without_mutation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();
        std::fs::write(data_path.join("foreign.bin"), b"do not touch").unwrap();
        let before = directory_bytes(&data_path);

        let err = build_error(builder(&data_path));
        assert!(matches!(err, TsinkError::InvalidConfiguration(message)
            if message.contains("refusing to guess")));
        assert_eq!(directory_bytes(&data_path), before);
        assert!(!data_path.join(".tsink.lock").exists());
        assert!(!manifest_path(&data_path).exists());
    }

    #[test]
    fn corrupt_known_legacy_file_is_rejected_without_mutation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();
        std::fs::write(data_path.join(SERIES_INDEX_FILE_NAME), b"foreign bytes").unwrap();
        let before = directory_bytes(&data_path);

        let err = build_error(builder(&data_path));
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("read-only series-registry identity validation")));
        assert_eq!(directory_bytes(&data_path), before);
        assert!(!data_path.join(".tsink.lock").exists());
        assert!(!manifest_path(&data_path).exists());
    }

    #[test]
    fn immutable_configuration_mismatches_do_not_rewrite_manifest() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let storage = builder(&data_path).build().unwrap();
        storage.close().unwrap();
        let manifest_before = std::fs::read(manifest_path(&data_path)).unwrap();

        let mismatches = [
            builder(&data_path).with_timestamp_precision(TimestampPrecision::Milliseconds),
            builder(&data_path).with_chunk_points(3),
            builder(&data_path).with_partition_duration(Duration::from_secs(120)),
        ];
        for mismatched in mismatches {
            let err = build_error(mismatched);
            assert!(matches!(err, TsinkError::InvalidConfiguration(message)
                if message.contains("immutable configuration mismatch")));
            assert_eq!(
                std::fs::read(manifest_path(&data_path)).unwrap(),
                manifest_before
            );
        }
    }

    #[test]
    fn successful_reopen_updates_only_the_last_opened_version() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let storage = builder(&data_path).build().unwrap();
        storage.close().unwrap();
        let path = manifest_path(&data_path);
        rewrite_manifest(&path, |payload| {
            payload.last_successfully_opened_tsink_version = Some("0.0.0".to_string());
        });
        let before = read_manifest(&path, usize::MAX).unwrap();

        let reopened = builder(&data_path).build().unwrap();
        reopened.close().unwrap();

        let after = read_manifest(&path, usize::MAX).unwrap();
        assert_eq!(after.creating_tsink_version, before.creating_tsink_version);
        assert_eq!(
            after.last_successfully_opened_tsink_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn failed_startup_finalization_does_not_update_last_opened_version() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        {
            let storage = builder(&data_path).build().unwrap();
            storage
                .insert_rows(&[Row::new("finalize_failure", DataPoint::new(1, 1.0))])
                .unwrap();
            storage.close().unwrap();
        }
        let path = manifest_path(&data_path);
        rewrite_manifest(&path, |payload| {
            payload.last_successfully_opened_tsink_version = Some("0.0.0".to_string());
        });

        remove_registry_identity(&data_path);
        let write_failure = crate::engine::fs_utils::fail_tmp_write_after_bytes_once(
            data_path.join(SERIES_INDEX_FILE_NAME),
            0,
            std::io::ErrorKind::Other,
            "injected startup finalization registry write failure",
        );
        let err = build_error(builder(&data_path).with_background_threads_enabled_for_tests(false));
        drop(write_failure);
        assert!(
            err.to_string()
                .contains("injected startup finalization registry write failure"),
            "unexpected startup error: {err}"
        );
        assert_eq!(
            read_manifest(&path, usize::MAX)
                .unwrap()
                .last_successfully_opened_tsink_version
                .as_deref(),
            Some("0.0.0")
        );
    }

    #[test]
    fn manifest_replacement_file_sync_failure_preserves_prior_bytes_and_reopen_recovers() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        {
            let storage = builder(&data_path).build().unwrap();
            storage
                .insert_rows(&[Row::new("manifest_sync_failure", DataPoint::new(1, 7.0))])
                .unwrap();
            storage.close().unwrap();
        }
        let path = manifest_path(&data_path);
        rewrite_manifest(&path, |payload| {
            payload.last_successfully_opened_tsink_version = Some("0.0.0".to_string());
        });
        let bytes_before = std::fs::read(&path).unwrap();
        let manifest_parent = data_path.clone();
        let failure = crate::engine::fs_utils::fail_file_sync_matching_once(
            move |candidate| {
                candidate.parent() == Some(manifest_parent.as_path())
                    && candidate.file_name().is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with(".tsink-manifest.json.tmp-")
                    })
            },
            "injected data-directory manifest file sync failure",
        );

        let err = build_error(builder(&data_path).with_background_threads_enabled_for_tests(false));
        drop(failure);
        assert!(
            err.to_string()
                .contains("injected data-directory manifest file sync failure"),
            "unexpected manifest replacement error: {err}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes_before,
            "a pre-replacement file-sync failure must preserve the prior manifest byte-for-byte"
        );
        assert!(
            std::fs::read_dir(&data_path).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".tsink-manifest.json.tmp-")
            }),
            "failed manifest replacement must clean its owned temporary"
        );

        let reopened = builder(&data_path)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        assert_eq!(
            reopened
                .select("manifest_sync_failure", &[], 0, 10)
                .unwrap(),
            vec![DataPoint::new(1, 7.0)]
        );
        reopened.close().unwrap();
        assert_eq!(
            read_manifest(&path, usize::MAX)
                .unwrap()
                .last_successfully_opened_tsink_version
                .as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn snapshot_and_restore_copy_the_manifest_byte_for_byte() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let snapshot_path = temp.path().join("snapshot");
        let restored_path = temp.path().join("restored");
        let storage = builder(&data_path).build().unwrap();
        storage
            .insert_rows(&[Row::new("snapshot_manifest", DataPoint::new(1, 3.0))])
            .unwrap();
        storage.snapshot(&snapshot_path).unwrap();
        storage.close().unwrap();

        assert_eq!(
            std::fs::read(manifest_path(&snapshot_path)).unwrap(),
            std::fs::read(manifest_path(&data_path)).unwrap()
        );
        StorageBuilder::restore_from_snapshot(&snapshot_path, &restored_path).unwrap();
        assert_eq!(
            std::fs::read(manifest_path(&restored_path)).unwrap(),
            std::fs::read(manifest_path(&snapshot_path)).unwrap()
        );
        let restored = builder(&restored_path).build().unwrap();
        assert_eq!(
            restored.select("snapshot_manifest", &[], 0, 2).unwrap(),
            vec![DataPoint::new(1, 3.0)]
        );
        restored.close().unwrap();
    }
}
