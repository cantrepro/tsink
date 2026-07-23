use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use super::value_family::{
    decode_optional_series_value_family, encode_optional_series_value_family,
};
use super::*;
use crate::engine::binio::{
    append_u16, append_u32, append_u64, checksum32, decode_optional_zstd_framed_file_with_limit,
    encode_optional_zstd_framed_file, read_array, read_bytes, read_to_end_bounded, read_u16,
    read_u32, read_u64, FILE_FLAG_ZSTD_BODY, MAX_DECODED_FRAMED_FILE_BYTES,
};
use crate::engine::fs_utils::{
    path_exists_no_follow, remove_empty_dir_if_exists,
    remove_path_if_exists_and_sync_parent_budgeted, rename_and_sync_parents, sync_parent_dir,
    write_file_atomically_and_sync_parent_budgeted,
};
use crate::{Result, TsinkError};

const REGISTRY_INDEX_HEADER_LEN: usize = 52;
const REGISTRY_DICTIONARY_ENTRY_HEADER_LEN: usize = 8;
const REGISTRY_SERIES_ENTRY_HEADER_LEN: usize = 16;
const REGISTRY_LABEL_PAIR_LEN: usize = 8;
/// Covers the decoded file, dictionary/string duplication, series-key duplication, rebuilt
/// roaring postings, and the temporary decoded representation. The physical file is charged
/// separately because compressed input and decoded output coexist during loading.
const STARTUP_REGISTRY_DECODE_RETAIN_FACTOR: usize = 12;
const STARTUP_REGISTRY_PATH_BOOKKEEPING_WORDS: usize = 4;
const REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IncrementalJournalHeader {
    series_count: usize,
    payload_len: usize,
    decoded_payload_len: usize,
    payload_crc32: u32,
}

fn encoded_registry_decoded_len(registry_payload: &[u8]) -> Result<usize> {
    if registry_payload.len() < 8 || registry_payload[..4] != REGISTRY_INDEX_MAGIC {
        return Err(TsinkError::DataCorruption(
            "incremental registry journal payload is not a series index".to_string(),
        ));
    }
    let flags = u16::from_le_bytes([registry_payload[6], registry_payload[7]]);
    let decoded_len = if flags & FILE_FLAG_ZSTD_BODY != 0 {
        if registry_payload.len() < 12 {
            return Err(TsinkError::DataCorruption(
                "compressed incremental registry journal payload is too short for its decoded-length prefix"
                    .to_string(),
            ));
        }
        let body_len = u32::from_le_bytes([
            registry_payload[8],
            registry_payload[9],
            registry_payload[10],
            registry_payload[11],
        ]) as usize;
        8usize.checked_add(body_len).ok_or_else(|| {
            TsinkError::DataCorruption(
                "incremental registry journal decoded length overflow".to_string(),
            )
        })?
    } else {
        registry_payload.len()
    };
    if decoded_len > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal decoded size {decoded_len} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    Ok(decoded_len)
}

fn try_registry_vec_with_capacity<T>(count: usize, context: &str) -> Result<Vec<T>> {
    let allocation_bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| TsinkError::DataCorruption(format!("{context} allocation overflow")))?;
    if allocation_bytes > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "{context} allocation {allocation_bytes} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|err| {
        TsinkError::Other(format!(
            "failed to reserve {count} entries while decoding {context}: {err}"
        ))
    })?;
    Ok(values)
}

fn read_registry_file_bounded(path: &Path, runtime_limit_bytes: usize) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let declared_len = file.metadata()?.len();
    let max_bytes = MAX_DECODED_FRAMED_FILE_BYTES.min(runtime_limit_bytes);
    if declared_len > max_bytes as u64 {
        let limit_kind = if max_bytes == MAX_DECODED_FRAMED_FILE_BYTES {
            "format safety limit"
        } else {
            "active decode limit"
        };
        return Err(TsinkError::DataCorruption(format!(
            "series index file size {declared_len} exceeds the {limit_kind} {max_bytes}"
        )));
    }

    let initial_capacity = usize::try_from(declared_len)
        .unwrap_or(max_bytes)
        .min(max_bytes);
    read_to_end_bounded(&mut file, max_bytes, initial_capacity, "series index")
}

#[derive(Debug, Clone, Copy)]
struct StartupRegistryFileShape {
    stored_bytes: usize,
    decoded_bytes: usize,
}

impl StartupRegistryFileShape {
    fn reservation_bytes(self) -> usize {
        self.stored_bytes.saturating_add(
            self.decoded_bytes
                .saturating_mul(STARTUP_REGISTRY_DECODE_RETAIN_FACTOR),
        )
    }
}

fn inspect_startup_registry_file(path: &Path) -> Result<Option<StartupRegistryFileShape>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let stored_len_u64 = file.metadata()?.len();
    if stored_len_u64 > MAX_DECODED_FRAMED_FILE_BYTES as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "series index file size {stored_len_u64} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    let stored_bytes = usize::try_from(stored_len_u64).map_err(|_| {
        TsinkError::DataCorruption("series index file size does not fit this platform".to_string())
    })?;

    let mut prefix = [0u8; 12];
    let prefix_len = stored_bytes.min(prefix.len());
    file.read_exact(&mut prefix[..prefix_len])?;
    let decoded_bytes = if prefix_len >= 8
        && u16::from_le_bytes([prefix[6], prefix[7]]) & FILE_FLAG_ZSTD_BODY != 0
    {
        if prefix_len < prefix.len() {
            return Err(TsinkError::DataCorruption(
                "compressed series index is too short for its decoded-length prefix".to_string(),
            ));
        }
        let body_len = u32::from_le_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]) as usize;
        8usize.checked_add(body_len).ok_or_else(|| {
            TsinkError::DataCorruption("series index decoded length overflow".to_string())
        })?
    } else {
        stored_bytes
    };
    if decoded_bytes > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "series index decoded size {decoded_bytes} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }

    Ok(Some(StartupRegistryFileShape {
        stored_bytes,
        decoded_bytes,
    }))
}

fn encode_incremental_journal(series_count: usize, registry_payload: &[u8]) -> Result<Vec<u8>> {
    let series_count = u64::try_from(series_count).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "incremental registry journal series count exceeds u64".to_string(),
        )
    })?;
    let payload_len = u64::try_from(registry_payload.len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "incremental registry journal payload length exceeds u64".to_string(),
        )
    })?;
    if registry_payload.len() > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "incremental registry journal payload size {} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}",
            registry_payload.len()
        )));
    }
    let decoded_payload_len = u32::try_from(encoded_registry_decoded_len(registry_payload)?)
        .map_err(|_| {
            TsinkError::InvalidConfiguration(
                "incremental registry journal decoded payload length exceeds u32".to_string(),
            )
        })?;

    let total_len = REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN
        .checked_add(registry_payload.len())
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "incremental registry journal length overflow".to_string(),
            )
        })?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(&REGISTRY_INCREMENTAL_JOURNAL_MAGIC);
    append_u16(&mut bytes, REGISTRY_INCREMENTAL_JOURNAL_VERSION);
    append_u16(&mut bytes, 0);
    append_u64(&mut bytes, series_count);
    append_u64(&mut bytes, payload_len);
    append_u32(&mut bytes, checksum32(registry_payload));
    append_u32(&mut bytes, decoded_payload_len);
    debug_assert_eq!(bytes.len(), REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN);
    bytes.extend_from_slice(registry_payload);
    Ok(bytes)
}

fn parse_incremental_journal_header(bytes: &[u8]) -> Result<IncrementalJournalHeader> {
    if bytes.len() < REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "incremental registry journal header is truncated".to_string(),
        ));
    }
    let mut pos = 0usize;
    if read_array::<4>(bytes, &mut pos)? != REGISTRY_INCREMENTAL_JOURNAL_MAGIC {
        return Err(TsinkError::DataCorruption(
            "incremental registry journal magic mismatch".to_string(),
        ));
    }
    let version = read_u16(bytes, &mut pos)?;
    if version != REGISTRY_INCREMENTAL_JOURNAL_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported incremental registry journal version {version}"
        )));
    }
    let flags = read_u16(bytes, &mut pos)?;
    if flags != 0 {
        return Err(TsinkError::DataCorruption(format!(
            "invalid incremental registry journal flags {flags:#06x}"
        )));
    }
    let series_count = usize::try_from(read_u64(bytes, &mut pos)?).map_err(|_| {
        TsinkError::DataCorruption(
            "incremental registry journal series count does not fit this platform".to_string(),
        )
    })?;
    let payload_len = usize::try_from(read_u64(bytes, &mut pos)?).map_err(|_| {
        TsinkError::DataCorruption(
            "incremental registry journal payload length does not fit this platform".to_string(),
        )
    })?;
    if payload_len > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal payload size {payload_len} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    let payload_crc32 = read_u32(bytes, &mut pos)?;
    let decoded_payload_len = read_u32(bytes, &mut pos)? as usize;
    if decoded_payload_len > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal decoded payload size {decoded_payload_len} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    debug_assert_eq!(pos, REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN);
    Ok(IncrementalJournalHeader {
        series_count,
        payload_len,
        decoded_payload_len,
        payload_crc32,
    })
}

fn read_incremental_journal_header(path: &Path) -> Result<Option<(IncrementalJournalHeader, u64)>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal is link-like or not a regular file: {}",
            path.display()
        )));
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal disappeared after validation: {}",
                path.display()
            )))
        }
        Err(err) => return Err(err.into()),
    };
    let stored_len = metadata.len();
    let max_stored_len = (REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN as u64)
        .saturating_add(MAX_DECODED_FRAMED_FILE_BYTES as u64);
    if stored_len > max_stored_len {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal file size {stored_len} exceeds the format safety limit {max_stored_len}"
        )));
    }
    let mut header_bytes = [0u8; REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN];
    file.read_exact(&mut header_bytes).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "failed to read incremental registry journal header {}: {err}",
            path.display()
        ))
    })?;
    let header = parse_incremental_journal_header(&header_bytes)?;
    let expected_len = (REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN as u64)
        .checked_add(header.payload_len as u64)
        .ok_or_else(|| {
            TsinkError::DataCorruption(
                "incremental registry journal file length overflow".to_string(),
            )
        })?;
    if expected_len != stored_len {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal length mismatch: header declares {expected_len} bytes, file has {stored_len}"
        )));
    }
    Ok(Some((header, stored_len)))
}

fn inspect_startup_registry_journal(path: &Path) -> Result<Option<StartupRegistryFileShape>> {
    let Some((header, stored_len)) = read_incremental_journal_header(path)? else {
        return Ok(None);
    };
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(
        REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN as u64,
    ))?;
    let prefix_len = header.payload_len.min(12);
    let mut prefix = [0u8; 12];
    file.read_exact(&mut prefix[..prefix_len])?;
    if prefix_len < 8 || prefix[..4] != REGISTRY_INDEX_MAGIC {
        return Err(TsinkError::DataCorruption(
            "incremental registry journal payload is not a series index".to_string(),
        ));
    }
    let decoded_bytes = if u16::from_le_bytes([prefix[6], prefix[7]]) & FILE_FLAG_ZSTD_BODY != 0 {
        encoded_registry_decoded_len(&prefix[..prefix_len])?
    } else {
        header.payload_len
    };
    if decoded_bytes != header.decoded_payload_len {
        return Err(TsinkError::DataCorruption(format!(
            "incremental registry journal decoded length mismatch: header declares {}, payload declares {decoded_bytes}",
            header.decoded_payload_len
        )));
    }
    let stored_bytes = usize::try_from(stored_len).map_err(|_| {
        TsinkError::DataCorruption(
            "incremental registry journal file size does not fit this platform".to_string(),
        )
    })?;
    Ok(Some(StartupRegistryFileShape {
        stored_bytes,
        decoded_bytes,
    }))
}

fn incremental_journal_payload_equals(
    path: &Path,
    header: IncrementalJournalHeader,
    expected_payload: &[u8],
) -> Result<bool> {
    if header.payload_len != expected_payload.len()
        || header.payload_crc32 != checksum32(expected_payload)
    {
        return Ok(false);
    }

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(
        REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN as u64,
    ))?;
    let mut offset = 0usize;
    let mut buffer = [0u8; 16 * 1024];
    while offset < expected_payload.len() {
        let chunk_len = buffer.len().min(expected_payload.len() - offset);
        file.read_exact(&mut buffer[..chunk_len])?;
        if buffer[..chunk_len] != expected_payload[offset..offset + chunk_len] {
            return Ok(false);
        }
        offset += chunk_len;
    }
    Ok(true)
}

fn startup_registry_path_reservation_bytes(path: &Path) -> usize {
    std::mem::size_of::<PathBuf>()
        .saturating_add(path.as_os_str().len())
        .saturating_add(
            STARTUP_REGISTRY_PATH_BOOKKEEPING_WORDS.saturating_mul(std::mem::size_of::<usize>()),
        )
}

impl SeriesRegistry {
    pub fn incremental_path(snapshot_path: &Path) -> PathBuf {
        snapshot_path
            .parent()
            .map(|parent| parent.join(REGISTRY_INCREMENTAL_FILE_NAME))
            .unwrap_or_else(|| PathBuf::from(REGISTRY_INCREMENTAL_FILE_NAME))
    }

    pub fn incremental_dir(snapshot_path: &Path) -> PathBuf {
        snapshot_path
            .parent()
            .map(|parent| parent.join(REGISTRY_INCREMENTAL_DIR_NAME))
            .unwrap_or_else(|| PathBuf::from(REGISTRY_INCREMENTAL_DIR_NAME))
    }

    /// Removes only tsink-owned incremental generations. Unrecognized entries and link-like
    /// objects are deliberately retained, and therefore also keep the directory itself alive.
    pub(crate) fn remove_incremental_segments_preserving_unknown(
        dir_path: &Path,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        let metadata = match fs::symlink_metadata(dir_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry path is link-like or not a directory: {}",
                dir_path.display()
            )));
        }

        let entries = crate::engine::fs_utils::collect_directory_entries_bounded(
            dir_path,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
            "incremental series registry checkpoint cleanup",
        )?;
        for entry in entries {
            let file_type = entry.file_type()?;
            if !file_type.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            if !Self::is_incremental_source_name(name) {
                continue;
            }
            remove_path_if_exists_and_sync_parent_budgeted(
                &entry.path(),
                local_disk_budget,
                crate::DiskCategory::Registry,
            )?;
        }

        match remove_empty_dir_if_exists(dir_path) {
            Ok(true) => sync_parent_dir(dir_path),
            Ok(false) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn load_incremental_state(snapshot_path: &Path) -> Result<Option<LoadedSeriesRegistry>> {
        let legacy_delta_path = Self::incremental_path(snapshot_path);
        let legacy_delta = Self::load_optional_from_path(&legacy_delta_path)?.map(|registry| {
            LoadedSeriesRegistry {
                delta_series_count: registry.series_count(),
                registry,
            }
        });
        let segmented_delta = Self::load_incremental_segments(snapshot_path)?;

        let mut loaded = legacy_delta;
        if let Some(segmented_delta) = segmented_delta {
            Self::merge_loaded_registry(&mut loaded, segmented_delta)?;
        }

        Ok(loaded)
    }

    pub fn load_persisted_state(snapshot_path: &Path) -> Result<Option<LoadedSeriesRegistry>> {
        let snapshot = Self::load_optional_from_path(snapshot_path)?;
        let incremental = Self::load_incremental_state(snapshot_path)?;

        let mut loaded = snapshot.map(|registry| LoadedSeriesRegistry {
            registry,
            delta_series_count: 0,
        });
        if let Some(incremental) = incremental {
            Self::merge_loaded_registry(&mut loaded, incremental)?;
        }

        Ok(loaded)
    }

    /// Loads the complete checkpoint + incremental registry set while admitting every retained
    /// path and every file's conservative decode/retained peak into one caller-owned ledger.
    ///
    /// The callback is invoked before a file body is read or decoded. It must return a structured
    /// resource error when the aggregate cannot be admitted. `runtime_decode_limit_bytes` is an
    /// additional per-file ceiling; callers normally pass the configured startup memory budget.
    pub(crate) fn load_persisted_state_with_startup_admission<Admit>(
        snapshot_path: &Path,
        runtime_decode_limit_bytes: usize,
        mut admit: Admit,
    ) -> Result<Option<LoadedSeriesRegistry>>
    where
        Admit: FnMut(usize) -> Result<()>,
    {
        let runtime_decode_limit_bytes =
            runtime_decode_limit_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES);
        let snapshot = Self::load_optional_from_path_with_startup_admission(
            snapshot_path,
            runtime_decode_limit_bytes,
            &mut admit,
        )?;
        let incremental = Self::load_incremental_state_with_startup_admission(
            snapshot_path,
            runtime_decode_limit_bytes,
            &mut admit,
        )?;

        let mut loaded = snapshot.map(|registry| LoadedSeriesRegistry {
            registry,
            delta_series_count: 0,
        });
        if let Some(incremental) = incremental {
            Self::merge_loaded_registry(&mut loaded, incremental)?;
        }
        Ok(loaded)
    }

    fn load_incremental_state_with_startup_admission<Admit>(
        snapshot_path: &Path,
        runtime_decode_limit_bytes: usize,
        admit: &mut Admit,
    ) -> Result<Option<LoadedSeriesRegistry>>
    where
        Admit: FnMut(usize) -> Result<()>,
    {
        let legacy_delta_path = Self::incremental_path(snapshot_path);
        let legacy_delta = Self::load_optional_from_path_with_startup_admission(
            &legacy_delta_path,
            runtime_decode_limit_bytes,
            admit,
        )?
        .map(|registry| LoadedSeriesRegistry {
            delta_series_count: registry.series_count(),
            registry,
        });
        let segmented_delta = Self::load_incremental_segments_with_startup_admission(
            snapshot_path,
            runtime_decode_limit_bytes,
            admit,
        )?;

        let mut loaded = legacy_delta;
        if let Some(segmented_delta) = segmented_delta {
            Self::merge_loaded_registry(&mut loaded, segmented_delta)?;
        }
        Ok(loaded)
    }

    #[cfg(test)]
    pub(crate) fn persist_incremental_to_snapshot_path(&self, snapshot_path: &Path) -> Result<()> {
        self.persist_incremental_to_snapshot_path_with_disk_budget_and_kind(
            snapshot_path,
            None,
            crate::DiskReservationKind::Maintenance,
        )
    }

    pub(crate) fn persist_incremental_to_snapshot_path_with_disk_budget_and_kind(
        &self,
        snapshot_path: &Path,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<()> {
        self.persist_incremental_to_snapshot_path_with_limits(
            snapshot_path,
            local_disk_budget,
            reservation_kind,
            REGISTRY_INCREMENTAL_JOURNAL_MAX_SERIES,
            REGISTRY_INCREMENTAL_JOURNAL_MAX_STORED_BYTES,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        )
    }

    fn persist_incremental_to_snapshot_path_with_limits(
        &self,
        snapshot_path: &Path,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        reservation_kind: crate::DiskReservationKind,
        max_active_series: usize,
        max_active_payload_bytes: usize,
        max_namespace_entries: usize,
    ) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        if max_active_series == 0 || max_active_payload_bytes == 0 {
            return Err(TsinkError::InvalidConfiguration(
                "incremental registry journal rollover limits must be non-zero".to_string(),
            ));
        }

        let dir_path = Self::incremental_dir(snapshot_path);
        let created_dir = match fs::symlink_metadata(&dir_path) {
            Ok(metadata) => {
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_dir()
                {
                    return Err(TsinkError::DataCorruption(format!(
                        "incremental registry path is link-like or not a directory: {}",
                        dir_path.display()
                    )));
                }
                false
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&dir_path)?;
                true
            }
            Err(err) => return Err(err.into()),
        };
        if created_dir {
            sync_parent_dir(&dir_path)?;
        }

        let incoming_payload = self.encoded_registry_bytes()?;
        let active_path = dir_path.join(REGISTRY_INCREMENTAL_JOURNAL_ACTIVE_FILE_NAME);
        let Some((active_header, _)) = read_incremental_journal_header(&active_path)? else {
            Self::ensure_incremental_namespace_capacity_with_limit(
                &dir_path,
                max_namespace_entries,
            )?;
            return Self::write_incremental_journal(
                &active_path,
                self.series_count(),
                &incoming_payload,
                local_disk_budget,
                reservation_kind,
            );
        };

        if active_header.series_count == self.series_count()
            && incremental_journal_payload_equals(&active_path, active_header, &incoming_payload)?
        {
            // A prior atomic publication may have committed before reporting a parent-sync or
            // accounting error. Re-publish the identical active generation so the caller may
            // safely clear its pending set without sealing a duplicate oversized generation.
            Self::ensure_incremental_namespace_capacity_with_limit(
                &dir_path,
                max_namespace_entries,
            )?;
            return Self::write_incremental_journal(
                &active_path,
                self.series_count(),
                &incoming_payload,
                local_disk_budget,
                reservation_kind,
            );
        }

        // Oversized selected batches are allowed as dedicated generations. Crucially, inspect
        // only their fixed-size wrapper on the following pass instead of decoding or merging the
        // potentially large registry payload in a bounded maintenance operation.
        let incoming_decoded_payload_len = encoded_registry_decoded_len(&incoming_payload)?;
        let active_is_mergeable = active_header.series_count < max_active_series
            && active_header.payload_len < max_active_payload_bytes
            && active_header.decoded_payload_len < max_active_payload_bytes
            && active_header
                .series_count
                .saturating_add(self.series_count())
                <= max_active_series
            && active_header
                .payload_len
                .saturating_add(incoming_payload.len())
                <= max_active_payload_bytes
            && active_header
                .decoded_payload_len
                .saturating_add(incoming_decoded_payload_len)
                <= max_active_payload_bytes;
        if active_is_mergeable {
            let merged = Self::load_incremental_journal_from_path_with_decoded_limit(
                &active_path,
                MAX_DECODED_FRAMED_FILE_BYTES,
            )?;
            merged.merge_from(self)?;
            let merged_payload = merged.encoded_registry_bytes()?;
            if merged.series_count() <= max_active_series
                && merged_payload.len() <= max_active_payload_bytes
                && encoded_registry_decoded_len(&merged_payload)? <= max_active_payload_bytes
            {
                // Atomic replacement temporarily owns a sibling directory entry. Admit that
                // crash-visible entry even though successful replacement leaves file count flat.
                Self::ensure_incremental_namespace_capacity_with_limit(
                    &dir_path,
                    max_namespace_entries,
                )?;
                return Self::write_incremental_journal(
                    &active_path,
                    merged.series_count(),
                    &merged_payload,
                    local_disk_budget,
                    reservation_kind,
                );
            }
        }

        Self::ensure_incremental_namespace_capacity_with_limit(&dir_path, max_namespace_entries)?;
        let sealed_path = Self::allocate_incremental_journal_segment_path(&dir_path)?;
        rename_and_sync_parents(&active_path, &sealed_path)?;
        Self::write_incremental_journal(
            &active_path,
            self.series_count(),
            &incoming_payload,
            local_disk_budget,
            reservation_kind,
        )
    }

    fn write_incremental_journal(
        path: &Path,
        series_count: usize,
        registry_payload: &[u8],
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<()> {
        let bytes = encode_incremental_journal(series_count, registry_payload)?;
        write_file_atomically_and_sync_parent_budgeted(
            path,
            &bytes,
            local_disk_budget,
            crate::DiskCategory::Registry,
            reservation_kind,
        )
    }

    pub fn persist_to_path(&self, path: &Path) -> Result<()> {
        self.persist_to_path_with_disk_budget(path, None)
    }

    pub(crate) fn persist_to_path_with_disk_budget(
        &self,
        path: &Path,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        self.persist_to_path_with_disk_budget_and_kind(
            path,
            local_disk_budget,
            crate::DiskReservationKind::Maintenance,
        )
    }

    pub(crate) fn persist_to_path_with_disk_budget_and_kind(
        &self,
        path: &Path,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let bytes = self.encoded_registry_bytes()?;
        write_file_atomically_and_sync_parent_budgeted(
            path,
            &bytes,
            local_disk_budget,
            crate::DiskCategory::Registry,
            reservation_kind,
        )?;
        Ok(())
    }

    fn encoded_registry_bytes(&self) -> Result<Vec<u8>> {
        let metric_dict = self.metric_dict.read();
        let label_name_dict = self.label_name_dict.read();
        let label_value_dict = self.label_value_dict.read();
        let mut series = self.series_definitions_with_families();
        series.sort_by_key(|(entry, _)| entry.series_id);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&REGISTRY_INDEX_MAGIC);
        append_u16(&mut bytes, REGISTRY_INDEX_VERSION);
        append_u16(&mut bytes, 0u16);
        append_u64(&mut bytes, self.next_series_id_value());
        append_u32(&mut bytes, metric_dict.len() as u32);
        append_u32(&mut bytes, label_name_dict.len() as u32);
        append_u32(&mut bytes, label_value_dict.len() as u32);
        append_u64(&mut bytes, series.len() as u64);
        append_u64(&mut bytes, REGISTRY_SECTION_VALUE_FAMILY);
        append_u64(&mut bytes, 0u64);

        for (id, value) in metric_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }
        for (id, value) in label_name_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }
        for (id, value) in label_value_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }

        for (entry, family) in &series {
            append_u64(&mut bytes, entry.series_id);
            append_u32(&mut bytes, entry.metric_id);
            let pair_count = u16::try_from(entry.label_pairs.len()).map_err(|_| {
                TsinkError::InvalidConfiguration(
                    "series label pair count exceeds u16 in registry index".to_string(),
                )
            })?;
            append_u16(&mut bytes, pair_count);
            append_u16(&mut bytes, encode_optional_series_value_family(*family));
            for pair in &entry.label_pairs {
                append_u32(&mut bytes, pair.name_id);
                append_u32(&mut bytes, pair.value_id);
            }
        }

        encode_optional_zstd_framed_file(&bytes)
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        Self::load_from_path_with_decoded_limit(path, MAX_DECODED_FRAMED_FILE_BYTES)
    }

    fn load_from_path_with_decoded_limit(
        path: &Path,
        runtime_decode_limit_bytes: usize,
    ) -> Result<Self> {
        let runtime_decode_limit_bytes =
            runtime_decode_limit_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES);
        let raw_bytes = read_registry_file_bounded(path, runtime_decode_limit_bytes)?;
        Self::load_from_encoded_registry_bytes(&raw_bytes, runtime_decode_limit_bytes)
    }

    fn load_from_encoded_registry_bytes(
        raw_bytes: &[u8],
        runtime_decode_limit_bytes: usize,
    ) -> Result<Self> {
        let runtime_decode_limit_bytes =
            runtime_decode_limit_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES);
        if raw_bytes.len() > runtime_decode_limit_bytes {
            return Err(TsinkError::DataCorruption(format!(
                "series index file size {} exceeds the active decode limit {runtime_decode_limit_bytes}",
                raw_bytes.len()
            )));
        }
        let bytes = decode_optional_zstd_framed_file_with_limit(
            raw_bytes,
            REGISTRY_INDEX_MAGIC,
            REGISTRY_INDEX_VERSION,
            "series index",
            runtime_decode_limit_bytes,
        )?;
        if bytes.len() < REGISTRY_INDEX_HEADER_LEN {
            return Err(TsinkError::DataCorruption(
                "series index file is too short".to_string(),
            ));
        }

        let mut pos = 0usize;
        let magic = read_array::<4>(&bytes, &mut pos)?;
        if magic != REGISTRY_INDEX_MAGIC {
            return Err(TsinkError::DataCorruption(
                "series index magic mismatch".to_string(),
            ));
        }

        let version = read_u16(&bytes, &mut pos)?;
        if version != REGISTRY_INDEX_VERSION {
            return Err(TsinkError::DataCorruption(format!(
                "unsupported series index version {version}"
            )));
        }

        let _flags = read_u16(&bytes, &mut pos)?;
        let next_series_id = read_u64(&bytes, &mut pos)?;
        let metric_count = read_u32(&bytes, &mut pos)? as usize;
        let label_name_count = read_u32(&bytes, &mut pos)? as usize;
        let label_value_count = read_u32(&bytes, &mut pos)? as usize;
        let series_count = usize::try_from(read_u64(&bytes, &mut pos)?).map_err(|_| {
            TsinkError::DataCorruption(
                "series index series count does not fit this platform".to_string(),
            )
        })?;
        let section_flags = read_u64(&bytes, &mut pos)?;
        if section_flags & !REGISTRY_SECTION_VALUE_FAMILY != 0 {
            return Err(TsinkError::DataCorruption(format!(
                "invalid series index section flags {section_flags:#018x}"
            )));
        }
        let _reserved_postings_count = read_u64(&bytes, &mut pos)?;

        let dictionary_count = metric_count
            .checked_add(label_name_count)
            .and_then(|count| count.checked_add(label_value_count))
            .ok_or_else(|| {
                TsinkError::DataCorruption("series index dictionary count overflow".to_string())
            })?;
        let minimum_dictionary_bytes = dictionary_count
            .checked_mul(REGISTRY_DICTIONARY_ENTRY_HEADER_LEN)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "series index dictionary byte length overflow".to_string(),
                )
            })?;
        let minimum_series_bytes = series_count
            .checked_mul(REGISTRY_SERIES_ENTRY_HEADER_LEN)
            .ok_or_else(|| {
                TsinkError::DataCorruption("series index entry byte length overflow".to_string())
            })?;
        let minimum_remaining = minimum_dictionary_bytes
            .checked_add(minimum_series_bytes)
            .ok_or_else(|| {
                TsinkError::DataCorruption("series index minimum length overflow".to_string())
            })?;
        if minimum_remaining > bytes.len().saturating_sub(pos) {
            return Err(TsinkError::DataCorruption(format!(
                "series index declared counts require at least {minimum_remaining} bytes, but only {} remain",
                bytes.len().saturating_sub(pos)
            )));
        }

        let metric_values = parse_dictionary(&bytes, &mut pos, metric_count)?;
        let label_name_values = parse_dictionary(&bytes, &mut pos, label_name_count)?;
        let label_value_values = parse_dictionary(&bytes, &mut pos, label_value_count)?;

        let metric_dict = StringDictionary::from_values(metric_values, "metric")?;
        let label_name_dict = StringDictionary::from_values(label_name_values, "label name")?;
        let label_value_dict = StringDictionary::from_values(label_value_values, "label value")?;

        let mut definitions =
            try_registry_vec_with_capacity(series_count, "series index definitions")?;
        for _ in 0..series_count {
            let series_id = read_u64(&bytes, &mut pos)?;
            let metric_id = read_u32(&bytes, &mut pos)?;
            let pair_count = read_u16(&bytes, &mut pos)? as usize;
            let value_family = if section_flags & REGISTRY_SECTION_VALUE_FAMILY != 0 {
                decode_optional_series_value_family(read_u16(&bytes, &mut pos)?)?
            } else {
                let _reserved = read_u16(&bytes, &mut pos)?;
                None
            };
            let pair_bytes = pair_count
                .checked_mul(REGISTRY_LABEL_PAIR_LEN)
                .ok_or_else(|| {
                    TsinkError::DataCorruption(
                        "series index label-pair byte length overflow".to_string(),
                    )
                })?;
            if pair_bytes > bytes.len().saturating_sub(pos) {
                return Err(TsinkError::DataCorruption(format!(
                    "series index label-pair block needs {pair_bytes} bytes, but only {} remain",
                    bytes.len().saturating_sub(pos)
                )));
            }
            let mut label_pairs =
                try_registry_vec_with_capacity(pair_count, "series index label pairs")?;
            for _ in 0..pair_count {
                let name_id = read_u32(&bytes, &mut pos)?;
                let value_id = read_u32(&bytes, &mut pos)?;
                label_pairs.push(LabelPairId { name_id, value_id });
            }
            label_pairs.sort_unstable();
            label_pairs.dedup();
            definitions.push((
                SeriesDefinition {
                    series_id,
                    metric_id,
                    label_pairs,
                },
                value_family,
            ));
        }

        if pos != bytes.len() {
            return Err(TsinkError::DataCorruption(
                "series index has trailing bytes".to_string(),
            ));
        }

        let registry = Self {
            next_series_id: AtomicU64::new(next_series_id.max(1)),
            pending_series_reservations: AtomicUsize::new(0),
            estimated_total_bytes: AtomicUsize::new(0),
            postings_generation: AtomicU64::new(0),
            metric_dict: RwLock::new(metric_dict),
            label_name_dict: RwLock::new(label_name_dict),
            label_value_dict: RwLock::new(label_value_dict),
            series_shards: std::array::from_fn(|_| RwLock::new(SeriesRegistryShard::default())),
            series_id_shards: std::array::from_fn(|_| RwLock::new(SeriesIdShardIndex::default())),
            all_series_shards: std::array::from_fn(|_| {
                RwLock::new(AllSeriesPostingsShard::default())
            }),
            metric_postings_shards: std::array::from_fn(|_| {
                RwLock::new(MetricPostingsShard::default())
            }),
            label_postings_shards: std::array::from_fn(|_| {
                RwLock::new(LabelPostingsShard::default())
            }),
            series_count: AtomicUsize::new(definitions.len()),
        };
        for (definition, value_family) in definitions {
            let series_id = definition.series_id;
            let key = SeriesKeyIds {
                metric_id: definition.metric_id,
                label_pairs: definition.label_pairs.clone(),
            };
            let shard_idx = Self::key_shard_idx_for_key(&key);
            {
                let mut shard = registry.series_shards[shard_idx].write();
                shard.estimated_series_bytes = shard.estimated_series_bytes.saturating_add(
                    Self::series_label_pairs_memory_bytes(&definition.label_pairs),
                );
                if let Some(value_family) = value_family {
                    shard.value_families.insert(series_id, value_family);
                    shard.estimated_series_bytes = shard
                        .estimated_series_bytes
                        .saturating_add(Self::value_family_entry_bytes());
                }
                shard.by_key.insert(key, series_id);
                shard.by_id.insert(series_id, definition);
            }
            registry.series_id_shards[Self::series_id_index_shard_idx(series_id)]
                .write()
                .series_id_to_registry_shard
                .insert(series_id, shard_idx);
        }
        registry.rebuild_postings_indexes();
        Ok(registry)
    }

    fn is_incremental_journal_path(path: &Path) -> bool {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        name == REGISTRY_INCREMENTAL_JOURNAL_ACTIVE_FILE_NAME
            || (name.starts_with(REGISTRY_INCREMENTAL_JOURNAL_SEGMENT_PREFIX)
                && name.ends_with(REGISTRY_INCREMENTAL_SEGMENT_SUFFIX))
    }

    fn is_incremental_source_name(name: &str) -> bool {
        fn has_exact_nonce(name: &str, prefix: &str) -> bool {
            name.strip_prefix(prefix)
                .and_then(|value| value.strip_suffix(REGISTRY_INCREMENTAL_SEGMENT_SUFFIX))
                .is_some_and(|nonce| {
                    nonce.len() == 16
                        && nonce
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        }

        name == REGISTRY_INCREMENTAL_JOURNAL_ACTIVE_FILE_NAME
            || has_exact_nonce(name, REGISTRY_INCREMENTAL_SEGMENT_PREFIX)
            || has_exact_nonce(name, REGISTRY_INCREMENTAL_JOURNAL_SEGMENT_PREFIX)
    }

    fn load_incremental_journal_from_path_with_decoded_limit(
        path: &Path,
        runtime_decode_limit_bytes: usize,
    ) -> Result<Self> {
        let runtime_decode_limit_bytes =
            runtime_decode_limit_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES);
        let Some((expected_header, stored_len)) = read_incremental_journal_header(path)? else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "incremental registry journal does not exist: {}",
                    path.display()
                ),
            )
            .into());
        };
        if expected_header.payload_len > runtime_decode_limit_bytes {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal payload size {} exceeds the active decode limit {runtime_decode_limit_bytes}",
                expected_header.payload_len
            )));
        }
        let max_stored_bytes = REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN
            .checked_add(runtime_decode_limit_bytes)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "incremental registry journal read limit overflow".to_string(),
                )
            })?;
        let initial_capacity = usize::try_from(stored_len)
            .unwrap_or(max_stored_bytes)
            .min(max_stored_bytes);
        let mut file = File::open(path)?;
        let bytes = read_to_end_bounded(
            &mut file,
            max_stored_bytes,
            initial_capacity,
            "incremental series registry journal",
        )?;
        let header = parse_incremental_journal_header(&bytes)?;
        if header != expected_header {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal header changed while reading {}",
                path.display()
            )));
        }
        let payload = bytes
            .get(REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN..)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "incremental registry journal payload is truncated".to_string(),
                )
            })?;
        if payload.len() != header.payload_len {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal payload length mismatch: header declares {}, decoded {}",
                header.payload_len,
                payload.len()
            )));
        }
        let actual_crc32 = checksum32(payload);
        if actual_crc32 != header.payload_crc32 {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal payload checksum mismatch: expected {:08x}, got {actual_crc32:08x}",
                header.payload_crc32
            )));
        }
        let actual_decoded_payload_len = encoded_registry_decoded_len(payload)?;
        if actual_decoded_payload_len != header.decoded_payload_len {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal decoded length mismatch: header declares {}, payload declares {actual_decoded_payload_len}",
                header.decoded_payload_len
            )));
        }
        let registry = Self::load_from_encoded_registry_bytes(payload, runtime_decode_limit_bytes)?;
        if registry.series_count() != header.series_count {
            return Err(TsinkError::DataCorruption(format!(
                "incremental registry journal series count mismatch: header declares {}, payload contains {}",
                header.series_count,
                registry.series_count()
            )));
        }
        Ok(registry)
    }

    fn load_incremental_source_with_decoded_limit(
        path: &Path,
        runtime_decode_limit_bytes: usize,
    ) -> Result<Self> {
        if Self::is_incremental_journal_path(path) {
            Self::load_incremental_journal_from_path_with_decoded_limit(
                path,
                runtime_decode_limit_bytes,
            )
        } else {
            Self::load_from_path_with_decoded_limit(path, runtime_decode_limit_bytes)
        }
    }

    fn inspect_startup_incremental_source(path: &Path) -> Result<Option<StartupRegistryFileShape>> {
        if Self::is_incremental_journal_path(path) {
            inspect_startup_registry_journal(path)
        } else {
            inspect_startup_registry_file(path)
        }
    }

    fn load_optional_from_path(path: &Path) -> Result<Option<Self>> {
        match Self::load_from_path(path) {
            Ok(registry) => Ok(Some(registry)),
            Err(TsinkError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn load_optional_from_path_with_startup_admission<Admit>(
        path: &Path,
        runtime_decode_limit_bytes: usize,
        admit: &mut Admit,
    ) -> Result<Option<Self>>
    where
        Admit: FnMut(usize) -> Result<()>,
    {
        let Some(shape) = inspect_startup_registry_file(path)? else {
            return Ok(None);
        };
        admit(shape.reservation_bytes())?;
        Self::load_from_path_with_decoded_limit(path, runtime_decode_limit_bytes).map(Some)
    }

    fn load_incremental_segments(snapshot_path: &Path) -> Result<Option<LoadedSeriesRegistry>> {
        let segment_paths = Self::incremental_segment_paths(snapshot_path)?;
        if segment_paths.is_empty() {
            return Ok(None);
        }

        let mut loaded = LoadedSeriesRegistry {
            registry: Self::new(),
            delta_series_count: 0,
        };
        for path in segment_paths {
            let registry = Self::load_incremental_source_with_decoded_limit(
                &path,
                MAX_DECODED_FRAMED_FILE_BYTES,
            )
            .map_err(|err| {
                TsinkError::DataCorruption(format!(
                    "failed to load incremental registry segment {}: {err}",
                    path.display()
                ))
            })?;
            loaded.delta_series_count = loaded
                .delta_series_count
                .saturating_add(registry.series_count());
            loaded.registry.merge_from(&registry).map_err(|err| {
                TsinkError::DataCorruption(format!(
                    "failed to merge incremental registry segment {}: {err}",
                    path.display()
                ))
            })?;
        }

        Ok(Some(loaded))
    }

    fn load_incremental_segments_with_startup_admission<Admit>(
        snapshot_path: &Path,
        runtime_decode_limit_bytes: usize,
        admit: &mut Admit,
    ) -> Result<Option<LoadedSeriesRegistry>>
    where
        Admit: FnMut(usize) -> Result<()>,
    {
        let segment_paths =
            Self::incremental_segment_paths_with_startup_admission(snapshot_path, admit)?;
        if segment_paths.is_empty() {
            return Ok(None);
        }

        let mut loaded = LoadedSeriesRegistry {
            registry: Self::new(),
            delta_series_count: 0,
        };
        for path in segment_paths {
            let Some(shape) = Self::inspect_startup_incremental_source(&path)? else {
                return Err(TsinkError::DataCorruption(format!(
                    "incremental registry segment disappeared after discovery: {}",
                    path.display()
                )));
            };
            admit(shape.reservation_bytes())?;
            let registry =
                Self::load_incremental_source_with_decoded_limit(&path, runtime_decode_limit_bytes)
                    .map_err(|err| {
                        TsinkError::DataCorruption(format!(
                            "failed to load incremental registry segment {}: {err}",
                            path.display()
                        ))
                    })?;
            loaded.delta_series_count = loaded
                .delta_series_count
                .saturating_add(registry.series_count());
            loaded.registry.merge_from(&registry).map_err(|err| {
                TsinkError::DataCorruption(format!(
                    "failed to merge incremental registry segment {}: {err}",
                    path.display()
                ))
            })?;
        }
        Ok(Some(loaded))
    }

    fn incremental_segment_paths_with_startup_admission<Admit>(
        snapshot_path: &Path,
        admit: &mut Admit,
    ) -> Result<Vec<PathBuf>>
    where
        Admit: FnMut(usize) -> Result<()>,
    {
        let dir_path = Self::incremental_dir(snapshot_path);
        match fs::symlink_metadata(&dir_path) {
            Ok(metadata) => {
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_dir()
                {
                    return Err(TsinkError::DataCorruption(format!(
                        "incremental registry path is link-like or not a directory: {}",
                        dir_path.display()
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        }

        let mut paths = Vec::new();
        let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        );
        for entry in fs::read_dir(&dir_path).map_err(|source| TsinkError::IoWithPath {
            path: dir_path.clone(),
            source,
        })? {
            namespace_budget.observe_entry(&dir_path, "incremental series registry discovery")?;
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: dir_path.clone(),
                source,
            })?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            if !Self::is_incremental_source_name(name) {
                continue;
            }
            let path = entry.path();
            admit(startup_registry_path_reservation_bytes(&path))?;
            paths.push(path);
        }
        paths.sort();
        Ok(paths)
    }

    fn incremental_segment_paths(snapshot_path: &Path) -> Result<Vec<PathBuf>> {
        Self::incremental_segment_paths_with_limit(
            snapshot_path,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        )
    }

    fn incremental_segment_paths_with_limit(
        snapshot_path: &Path,
        max_entries: usize,
    ) -> Result<Vec<PathBuf>> {
        let dir_path = Self::incremental_dir(snapshot_path);
        match fs::symlink_metadata(&dir_path) {
            Ok(metadata) => {
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_dir()
                {
                    return Err(TsinkError::DataCorruption(format!(
                        "incremental registry path is link-like or not a directory: {}",
                        dir_path.display()
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        }

        let mut paths = Vec::new();
        let entries = crate::engine::fs_utils::collect_directory_entries_bounded(
            &dir_path,
            max_entries,
            "incremental series registry discovery",
        )?;
        for entry in entries {
            let file_type = entry.file_type()?;
            if !file_type.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            if !Self::is_incremental_source_name(name) {
                continue;
            }
            paths.push(entry.path());
        }
        paths.sort();
        Ok(paths)
    }

    fn allocate_incremental_journal_segment_path(dir_path: &Path) -> Result<PathBuf> {
        Self::allocate_incremental_segment_path_with_prefix_and_counter(
            dir_path,
            REGISTRY_INCREMENTAL_JOURNAL_SEGMENT_PREFIX,
            &REGISTRY_INCREMENTAL_SEGMENT_COUNTER,
        )
    }

    fn ensure_incremental_namespace_capacity_with_limit(
        dir_path: &Path,
        max_entries: usize,
    ) -> Result<()> {
        if max_entries == 0 {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "incremental series registry",
                limit: 0,
                required: 1,
            });
        }
        let mut observed_entries = 0usize;
        for entry in fs::read_dir(dir_path).map_err(|source| TsinkError::IoWithPath {
            path: dir_path.to_path_buf(),
            source,
        })? {
            entry.map_err(|source| TsinkError::IoWithPath {
                path: dir_path.to_path_buf(),
                source,
            })?;
            observed_entries = observed_entries.checked_add(1).ok_or_else(|| {
                TsinkError::Other(format!(
                    "incremental registry namespace entry counter overflow at {}",
                    dir_path.display()
                ))
            })?;
            if observed_entries >= max_entries {
                return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                    operation: "incremental series registry",
                    limit: max_entries,
                    required: observed_entries.saturating_add(1),
                });
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn allocate_incremental_segment_path_with_counter(
        dir_path: &Path,
        counter: &AtomicU64,
    ) -> Result<PathBuf> {
        Self::allocate_incremental_segment_path_with_prefix_and_counter(
            dir_path,
            REGISTRY_INCREMENTAL_SEGMENT_PREFIX,
            counter,
        )
    }

    fn allocate_incremental_segment_path_with_prefix_and_counter(
        dir_path: &Path,
        prefix: &str,
        counter: &AtomicU64,
    ) -> Result<PathBuf> {
        // The process-local counter restarts from one, while durable incremental files survive a
        // process restart. Search the complete bounded registry namespace rather than failing
        // after an arbitrary 256 collisions; once a free path is found the counter naturally
        // resumes beyond the durable prefix for later appends in this process.
        for _ in 0..crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES {
            let nonce = counter.fetch_add(1, Ordering::Relaxed);
            let candidate = dir_path.join(format!(
                "{prefix}{nonce:016x}{REGISTRY_INCREMENTAL_SEGMENT_SUFFIX}"
            ));
            if !path_exists_no_follow(&candidate)? {
                return Ok(candidate);
            }
        }

        Err(TsinkError::Other(format!(
            "failed to allocate incremental registry segment in {}",
            dir_path.display()
        )))
    }

    fn merge_loaded_registry(
        target: &mut Option<LoadedSeriesRegistry>,
        incoming: LoadedSeriesRegistry,
    ) -> Result<()> {
        if let Some(existing) = target.as_mut() {
            existing.registry.merge_from(&incoming.registry)?;
            existing.delta_series_count = existing
                .delta_series_count
                .saturating_add(incoming.delta_series_count);
            return Ok(());
        }

        *target = Some(incoming);
        Ok(())
    }
}

fn write_dict_entry(out: &mut Vec<u8>, id: u32, value: &str) -> Result<()> {
    let value_bytes = value.as_bytes();
    let len = u32::try_from(value_bytes.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("registry dictionary string exceeds u32".to_string())
    })?;
    append_u32(out, id);
    append_u32(out, len);
    out.extend_from_slice(value_bytes);
    Ok(())
}

fn parse_dictionary(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<String>> {
    let minimum_headers = count
        .checked_mul(REGISTRY_DICTIONARY_ENTRY_HEADER_LEN)
        .ok_or_else(|| {
            TsinkError::DataCorruption(
                "registry dictionary header byte length overflow".to_string(),
            )
        })?;
    if minimum_headers > bytes.len().saturating_sub(*pos) {
        return Err(TsinkError::DataCorruption(format!(
            "registry dictionary declares {count} entries requiring at least {minimum_headers} bytes, but only {} remain",
            bytes.len().saturating_sub(*pos)
        )));
    }
    let mut values = try_registry_vec_with_capacity(count, "registry dictionary")?;
    for expected_id in 0..count {
        let id = read_u32(bytes, pos)? as usize;
        if id != expected_id {
            return Err(TsinkError::DataCorruption(format!(
                "registry dictionary id {} is not dense at expected {}",
                id, expected_id
            )));
        }

        let len = read_u32(bytes, pos)? as usize;
        let value_bytes = read_bytes(bytes, pos, len)?;
        let value = String::from_utf8(value_bytes.to_vec())?;
        values.push(value);
    }
    Ok(values)
}

#[cfg(test)]
mod namespace_bound_tests {
    use super::*;
    use tempfile::TempDir;

    fn one_series_registry(series_id: SeriesId) -> SeriesRegistry {
        let registry = SeriesRegistry::new();
        registry
            .register_series_with_id(
                series_id,
                "cpu",
                &[crate::Label::new("host", format!("h-{series_id}"))],
            )
            .unwrap();
        registry
    }

    fn persist_with_test_limits(
        registry: &SeriesRegistry,
        snapshot_path: &Path,
        max_active_series: usize,
        max_active_payload_bytes: usize,
        max_namespace_entries: usize,
    ) -> Result<()> {
        registry.persist_incremental_to_snapshot_path_with_limits(
            snapshot_path,
            None,
            crate::DiskReservationKind::Maintenance,
            max_active_series,
            max_active_payload_bytes,
            max_namespace_entries,
        )
    }

    #[test]
    fn incremental_registry_compacts_repeated_tiny_deltas_into_bounded_generations() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        let incremental_dir = SeriesRegistry::incremental_dir(&snapshot_path);

        for series_id in 1..=10 {
            persist_with_test_limits(
                &one_series_registry(series_id),
                &snapshot_path,
                3,
                MAX_DECODED_FRAMED_FILE_BYTES,
                32,
            )
            .unwrap();
        }

        let paths = SeriesRegistry::incremental_segment_paths(&snapshot_path).unwrap();
        assert_eq!(
            paths.len(),
            4,
            "ten one-series writes should occupy ceil(10 / 3) generations"
        );
        assert!(incremental_dir
            .join(REGISTRY_INCREMENTAL_JOURNAL_ACTIVE_FILE_NAME)
            .exists());
        let loaded = SeriesRegistry::load_persisted_state(&snapshot_path)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.registry.series_count(), 10);
        assert_eq!(loaded.delta_series_count, 10);
        assert_eq!(
            loaded.registry.all_series_ids(),
            (1..=10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn incremental_registry_oversized_publication_retry_is_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        let registry = one_series_registry(1);

        persist_with_test_limits(
            &registry,
            &snapshot_path,
            1,
            MAX_DECODED_FRAMED_FILE_BYTES,
            8,
        )
        .unwrap();
        persist_with_test_limits(
            &registry,
            &snapshot_path,
            1,
            MAX_DECODED_FRAMED_FILE_BYTES,
            8,
        )
        .unwrap();

        assert_eq!(
            SeriesRegistry::incremental_segment_paths(&snapshot_path)
                .unwrap()
                .len(),
            1,
            "retrying the same dedicated generation must not seal a duplicate"
        );
        let loaded = SeriesRegistry::load_persisted_state(&snapshot_path)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.registry.all_series_ids(), vec![1]);
    }

    #[test]
    fn incremental_registry_rollover_interruption_restarts_from_sealed_generation() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        let incremental_dir = SeriesRegistry::incremental_dir(&snapshot_path);
        persist_with_test_limits(
            &one_series_registry(1),
            &snapshot_path,
            2,
            MAX_DECODED_FRAMED_FILE_BYTES,
            16,
        )
        .unwrap();
        persist_with_test_limits(
            &one_series_registry(2),
            &snapshot_path,
            2,
            MAX_DECODED_FRAMED_FILE_BYTES,
            16,
        )
        .unwrap();

        // Force the durable midpoint of rollover: rename has completed, the parent sync reports
        // failure, and the replacement active generation has not been published yet.
        let sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            incremental_dir.clone(),
            "injected journal rollover directory sync failure",
        );
        let err = persist_with_test_limits(
            &one_series_registry(3),
            &snapshot_path,
            2,
            MAX_DECODED_FRAMED_FILE_BYTES,
            16,
        )
        .expect_err("rollover sync failure must be surfaced");
        drop(sync_failure);
        assert!(
            err.to_string()
                .contains("injected journal rollover directory sync failure"),
            "unexpected rollover error: {err}"
        );
        fs::write(incremental_dir.join(".journal-active.bin.torn"), b"partial").unwrap();

        let midpoint = SeriesRegistry::load_persisted_state(&snapshot_path)
            .unwrap()
            .unwrap();
        assert_eq!(midpoint.registry.all_series_ids(), vec![1, 2]);

        persist_with_test_limits(
            &one_series_registry(3),
            &snapshot_path,
            2,
            MAX_DECODED_FRAMED_FILE_BYTES,
            16,
        )
        .unwrap();
        let restarted = SeriesRegistry::load_persisted_state(&snapshot_path)
            .unwrap()
            .unwrap();
        assert_eq!(restarted.registry.all_series_ids(), vec![1, 2, 3]);
        assert!(
            incremental_dir.join(".journal-active.bin.torn").exists(),
            "unrecognized crash debris must never be deleted"
        );
    }

    #[test]
    fn incremental_registry_rejects_torn_or_checksum_corrupt_managed_journal() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        persist_with_test_limits(
            &one_series_registry(1),
            &snapshot_path,
            2,
            MAX_DECODED_FRAMED_FILE_BYTES,
            16,
        )
        .unwrap();
        let active = SeriesRegistry::incremental_dir(&snapshot_path)
            .join(REGISTRY_INCREMENTAL_JOURNAL_ACTIVE_FILE_NAME);
        let original = fs::read(&active).unwrap();

        fs::write(
            &active,
            &original[..REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN - 1],
        )
        .unwrap();
        let torn = SeriesRegistry::load_persisted_state(&snapshot_path).unwrap_err();
        assert!(torn.to_string().contains("journal header"));

        let mut corrupt = original;
        corrupt[REGISTRY_INCREMENTAL_JOURNAL_HEADER_LEN] ^= 0xff;
        fs::write(&active, corrupt).unwrap();
        let checksum = SeriesRegistry::load_persisted_state(&snapshot_path).unwrap_err();
        assert!(checksum.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn incremental_registry_namespace_limit_stops_before_rollover_and_preserves_unknown_files() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        let incremental_dir = SeriesRegistry::incremental_dir(&snapshot_path);
        fs::create_dir_all(&incremental_dir).unwrap();
        let unknown = incremental_dir.join("host-owned");
        fs::write(&unknown, b"opaque").unwrap();

        persist_with_test_limits(
            &one_series_registry(1),
            &snapshot_path,
            1,
            MAX_DECODED_FRAMED_FILE_BYTES,
            2,
        )
        .unwrap();
        let err = persist_with_test_limits(
            &one_series_registry(2),
            &snapshot_path,
            1,
            MAX_DECODED_FRAMED_FILE_BYTES,
            2,
        )
        .expect_err("rollover must stop before startup's namespace limit is exceeded");
        assert!(matches!(
            err,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "incremental series registry",
                limit: 2,
                required: 3,
            }
        ));
        assert!(unknown.exists());
        let loaded = SeriesRegistry::load_persisted_state(&snapshot_path)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.registry.all_series_ids(), vec![1]);
    }

    #[test]
    fn incremental_registry_checkpoint_cleanup_preserves_unknown_entries() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        let incremental_dir = SeriesRegistry::incremental_dir(&snapshot_path);
        persist_with_test_limits(
            &one_series_registry(1),
            &snapshot_path,
            1,
            MAX_DECODED_FRAMED_FILE_BYTES,
            8,
        )
        .unwrap();
        persist_with_test_limits(
            &one_series_registry(2),
            &snapshot_path,
            1,
            MAX_DECODED_FRAMED_FILE_BYTES,
            8,
        )
        .unwrap();
        let unknown = incremental_dir.join("host-owned");
        let lookalike = incremental_dir.join("delta-0001.bin");
        fs::write(&unknown, b"opaque").unwrap();
        fs::write(&lookalike, b"lookalike").unwrap();

        SeriesRegistry::remove_incremental_segments_preserving_unknown(&incremental_dir, None)
            .unwrap();

        assert!(unknown.exists());
        assert!(lookalike.exists());
        assert!(incremental_dir.exists());
        assert!(SeriesRegistry::incremental_segment_paths(&snapshot_path)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn incremental_registry_rollover_and_cleanup_keep_disk_accounting_exact() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().join("data");
        let budget =
            crate::LocalDiskBudget::open(&data_path, crate::LocalDiskLimits::default()).unwrap();
        let snapshot_path = data_path.join("series_index.bin");
        let incremental_dir = SeriesRegistry::incremental_dir(&snapshot_path);

        for series_id in 1..=2 {
            one_series_registry(series_id)
                .persist_incremental_to_snapshot_path_with_limits(
                    &snapshot_path,
                    Some(&budget),
                    crate::DiskReservationKind::Maintenance,
                    1,
                    MAX_DECODED_FRAMED_FILE_BYTES,
                    8,
                )
                .unwrap();
        }
        let managed_bytes = fs::read_dir(&incremental_dir)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum::<u64>();
        let snapshot = budget.snapshot();
        let registry_bytes = snapshot
            .categories
            .iter()
            .find(|usage| usage.category == crate::DiskCategory::Registry)
            .map_or(0, |usage| usage.bytes);
        assert_eq!(registry_bytes, managed_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.reservation_overruns_total, 0);

        let unknown = incremental_dir.join("host-owned");
        fs::write(&unknown, b"opaque").unwrap();
        budget.reconcile().unwrap();
        SeriesRegistry::remove_incremental_segments_preserving_unknown(
            &incremental_dir,
            Some(&budget),
        )
        .unwrap();
        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == crate::DiskCategory::Registry)
                .map_or(0, |usage| usage.bytes),
            b"opaque".len() as u64,
            "the registry namespace conservatively owns bytes of preserved unknown entries"
        );
        assert_eq!(snapshot.unknown_bytes, 0);
        assert!(unknown.exists());
    }

    #[test]
    fn incremental_registry_discovery_counts_entries_before_name_filtering() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("series_index.bin");
        let incremental_dir = SeriesRegistry::incremental_dir(&snapshot_path);
        fs::create_dir_all(&incremental_dir).unwrap();
        fs::write(
            incremental_dir.join("delta-0000000000000001.bin"),
            b"segment",
        )
        .unwrap();
        fs::write(incremental_dir.join("host-owned"), b"opaque").unwrap();
        fs::write(incremental_dir.join("delta-0001.bin"), b"lookalike").unwrap();

        let paths = SeriesRegistry::incremental_segment_paths_with_limit(&snapshot_path, 3)
            .expect("the exact namespace cap must succeed");
        assert_eq!(
            paths,
            vec![incremental_dir.join("delta-0000000000000001.bin")]
        );

        fs::create_dir(incremental_dir.join("unknown-directory")).unwrap();
        let err = SeriesRegistry::incremental_segment_paths_with_limit(&snapshot_path, 3)
            .expect_err("cap plus one must fail before filtering");
        assert!(err.to_string().contains("3-entry global work bound"));
    }

    #[test]
    fn incremental_registry_path_allocation_survives_more_than_256_restart_collisions() {
        let temp_dir = TempDir::new().unwrap();
        let counter = AtomicU64::new(1);
        for nonce in 1..=300u64 {
            fs::write(
                temp_dir.path().join(format!(
                    "{REGISTRY_INCREMENTAL_SEGMENT_PREFIX}{nonce:016x}{REGISTRY_INCREMENTAL_SEGMENT_SUFFIX}"
                )),
                b"durable-delta",
            )
            .unwrap();
        }

        let allocated = SeriesRegistry::allocate_incremental_segment_path_with_counter(
            temp_dir.path(),
            &counter,
        )
        .unwrap();
        assert_eq!(
            allocated,
            temp_dir.path().join(format!(
                "{REGISTRY_INCREMENTAL_SEGMENT_PREFIX}{:016x}{REGISTRY_INCREMENTAL_SEGMENT_SUFFIX}",
                301u64
            ))
        );
    }

    #[test]
    fn incremental_registry_rejects_growth_before_exceeding_restart_namespace_bound() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join("delta-0001.bin"), b"durable-delta").unwrap();

        SeriesRegistry::ensure_incremental_namespace_capacity_with_limit(temp_dir.path(), 2)
            .expect("one existing entry leaves room for one atomic registry publication");

        fs::write(temp_dir.path().join("host-owned"), b"opaque").unwrap();
        let err =
            SeriesRegistry::ensure_incremental_namespace_capacity_with_limit(temp_dir.path(), 2)
                .expect_err("the runtime must not create a namespace startup cannot reopen");
        assert!(matches!(
            err,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "incremental series registry",
                limit: 2,
                required: 3,
            }
        ));
    }

    #[test]
    fn registry_loader_rejects_impossible_counts_before_capacity_reservation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("series_index.bin");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&REGISTRY_INDEX_MAGIC);
        append_u16(&mut bytes, REGISTRY_INDEX_VERSION);
        append_u16(&mut bytes, 0);
        append_u64(&mut bytes, 1);
        append_u32(&mut bytes, u32::MAX);
        append_u32(&mut bytes, 0);
        append_u32(&mut bytes, 0);
        append_u64(&mut bytes, 0);
        append_u64(&mut bytes, REGISTRY_SECTION_VALUE_FAMILY);
        append_u64(&mut bytes, 0);
        fs::write(&path, bytes).unwrap();

        let err = SeriesRegistry::load_from_path(&path).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("declared counts require at least")));
    }

    #[test]
    fn registry_loader_rejects_oversized_file_from_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("series_index.bin");
        File::create(&path)
            .unwrap()
            .set_len(MAX_DECODED_FRAMED_FILE_BYTES as u64 + 1)
            .unwrap();

        let err = SeriesRegistry::load_from_path(&path).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds the format safety limit")));
    }
}
