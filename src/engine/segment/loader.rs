use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use tracing::warn;

use crate::engine::binio::{
    read_to_end_bounded, FILE_FLAG_ZSTD_BODY, MAX_DECODED_FRAMED_FILE_BYTES,
};
use crate::engine::chunk::Chunk;
use crate::engine::index::ChunkIndex;
use crate::engine::series::SeriesId;
use crate::mmap::{create_mmap, PlatformMmap};
use crate::{Result, TsinkError};

use super::format::{
    decode_persisted_series, parse_chunk_index_file, parse_chunk_index_file_with_decoded_limit,
    parse_chunks_file, parse_manifest, parse_postings_file, parse_postings_file_with_decoded_limit,
    parse_series_file, parse_series_file_with_decoded_limit, segment_manifest_from_parsed,
    validate_chunk_index_against_chunks_file, verify_file_manifest_entry, ChunkRecordMeta,
    CHUNK_INDEX_MAGIC, FILE_KIND_CHUNKS, FILE_KIND_CHUNK_INDEX, FILE_KIND_POSTINGS,
    FILE_KIND_SERIES, FORMAT_VERSION, MAX_SEGMENT_CHUNKS_FILE_BYTES,
    MAX_SEGMENT_MANIFEST_FILE_BYTES, POSTINGS_MAGIC, SERIES_MAGIC,
};
use super::postings::SegmentPostingsIndex;
use super::types::{
    IndexedSegment, LoadedSegment, LoadedSegmentIndexes, LoadedSegments, PersistedSeries,
    SegmentContentFingerprint, SegmentFileFingerprint, SegmentLayout, SegmentManifest,
};
use super::validation::{
    handle_segment_dir_load_error, is_not_found_error, segment_validation_error_message,
    SegmentLoadPolicy, SegmentValidationContext,
};
use super::{quarantine_invalid_startup_segment, StartupQuarantinedSegment, WalHighWatermark};

#[derive(Debug)]
struct ParsedSegment {
    root: PathBuf,
    manifest: SegmentManifest,
    series: Vec<PersistedSeries>,
    chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

struct ParsedIndexedSegment {
    root: PathBuf,
    manifest: SegmentManifest,
    series: Vec<PersistedSeries>,
    postings: SegmentPostingsIndex,
    chunk_index: ChunkIndex,
    chunks_mmap: PlatformMmap,
}

#[derive(Clone, Copy)]
enum IndexedSegmentLoadMode {
    Standard(SegmentLoadPolicy),
    StartupRecoverable { metadata_decode_limit_bytes: usize },
}

enum IndexedSegmentLoadOutcome {
    Loaded(ParsedIndexedSegment),
    Skipped,
    Quarantined(StartupQuarantinedSegment),
}

pub(crate) struct StartupRecoveredSegmentIndexes {
    pub(crate) loaded: LoadedSegmentIndexes,
    pub(crate) quarantined: Vec<StartupQuarantinedSegment>,
}

pub fn load_segments(base: impl AsRef<Path>) -> Result<LoadedSegments> {
    load_segments_with_policy(base, SegmentLoadPolicy::SkipInvalid)
}

pub(crate) fn load_segments_runtime_strict(base: impl AsRef<Path>) -> Result<LoadedSegments> {
    load_segments_with_policy(
        base,
        SegmentLoadPolicy::FailOnInvalid(SegmentValidationContext::Compaction),
    )
}

fn load_segments_with_policy(
    base: impl AsRef<Path>,
    policy: SegmentLoadPolicy,
) -> Result<LoadedSegments> {
    let dirs = collect_segment_dirs(base.as_ref(), 0..=2u8)?;

    let mut parsed = Vec::new();
    let mut max_segment_id = 0u64;

    for dir in dirs {
        if let Some(segment) = load_segment_dir_with_policy(
            &dir,
            policy,
            "Segment directory disappeared during scan; skipping",
            "Ignoring invalid segment directory",
        )? {
            max_segment_id = max_segment_id.max(segment.manifest.segment_id);
            parsed.push(segment);
        }
    }

    parsed.sort_by_key(|segment| (segment.manifest.level, segment.manifest.segment_id));

    let mut series_by_id = BTreeMap::<SeriesId, PersistedSeries>::new();
    let mut chunks_by_series = HashMap::<SeriesId, Vec<Chunk>>::new();

    for segment in parsed {
        merge_persisted_series(&mut series_by_id, segment.series)?;

        for (series_id, mut chunks) in segment.chunks_by_series {
            chunks_by_series
                .entry(series_id)
                .or_default()
                .append(&mut chunks);
        }
    }

    for chunks in chunks_by_series.values_mut() {
        sort_chunks_by_time(chunks);
    }

    Ok(LoadedSegments {
        next_segment_id: max_segment_id.saturating_add(1).max(1),
        series: series_by_id.into_values().collect(),
        chunks_by_series,
    })
}

pub fn list_segment_dirs(base: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    collect_segment_dirs(base.as_ref(), 0..=2u8)
}

pub fn load_segment(path: impl AsRef<Path>) -> Result<LoadedSegment> {
    let segment = load_segment_dir(path.as_ref())?;
    Ok(LoadedSegment {
        root: segment.root,
        manifest: segment.manifest,
        series: segment.series,
        chunks_by_series: segment.chunks_by_series,
    })
}

pub fn load_segment_index(path: impl AsRef<Path>) -> Result<IndexedSegment> {
    load_segment_index_with_series(path, true)
}

pub fn load_segment_index_with_series(
    path: impl AsRef<Path>,
    load_series: bool,
) -> Result<IndexedSegment> {
    let segment = load_segment_dir_indexed(path.as_ref(), load_series)?;
    Ok(IndexedSegment {
        root: segment.root,
        manifest: segment.manifest,
        series: segment.series,
        postings: segment.postings,
        chunk_index: segment.chunk_index,
        chunks_mmap: segment.chunks_mmap,
    })
}

pub(crate) fn load_segment_series_metadata(path: impl AsRef<Path>) -> Result<Vec<PersistedSeries>> {
    let root = path.as_ref();
    let layout = SegmentLayout::from_root(root.to_path_buf());
    let manifest_bytes = read_required_segment_file(root, &layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;
    let series_bytes = read_required_segment_file(root, &layout.series_path)?;
    verify_file_manifest_entry(&parsed_manifest.files[2], FILE_KIND_SERIES, &series_bytes)?;
    let parsed_series = parse_series_file(&series_bytes)?;
    decode_persisted_series(&parsed_series)
}

pub fn load_segment_indexes(base: impl AsRef<Path>) -> Result<LoadedSegmentIndexes> {
    load_segment_indexes_with_series(base, true)
}

pub fn load_segment_indexes_with_series(
    base: impl AsRef<Path>,
    load_series: bool,
) -> Result<LoadedSegmentIndexes> {
    let dirs = collect_segment_dirs(base.as_ref(), 0..=2u8)?;
    load_segment_indexes_from_dirs_with_series(dirs, load_series)
}

pub fn load_segment_indexes_from_dirs_with_series(
    dirs: Vec<PathBuf>,
    load_series: bool,
) -> Result<LoadedSegmentIndexes> {
    load_segment_indexes_from_dirs_impl(
        dirs,
        load_series,
        IndexedSegmentLoadMode::Standard(SegmentLoadPolicy::SkipInvalid),
    )
}

pub fn load_segment_indexes_from_dirs_strict_with_series(
    dirs: Vec<PathBuf>,
    load_series: bool,
) -> Result<LoadedSegmentIndexes> {
    load_segment_indexes_from_dirs_impl(
        dirs,
        load_series,
        IndexedSegmentLoadMode::Standard(SegmentLoadPolicy::FailOnInvalid(
            SegmentValidationContext::Startup,
        )),
    )
}

pub(crate) fn load_segment_indexes_from_dirs_startup_recoverable_with_series(
    dirs: Vec<PathBuf>,
    load_series: bool,
    metadata_decode_limit_bytes: usize,
) -> Result<StartupRecoveredSegmentIndexes> {
    let mut parsed = Vec::new();
    let mut max_segment_id = 0u64;
    let mut max_wal_highwater = WalHighWatermark::default();
    let mut quarantined = Vec::new();

    for dir in dirs {
        match load_indexed_segment_dir_with_policy(
            &dir,
            load_series,
            IndexedSegmentLoadMode::StartupRecoverable {
                metadata_decode_limit_bytes,
            },
        )? {
            IndexedSegmentLoadOutcome::Loaded(segment) => {
                max_segment_id = max_segment_id.max(segment.manifest.segment_id);
                max_wal_highwater = max_wal_highwater.max(segment.manifest.wal_highwater);
                parsed.push(segment);
            }
            IndexedSegmentLoadOutcome::Skipped => {}
            IndexedSegmentLoadOutcome::Quarantined(segment) => quarantined.push(segment),
        }
    }

    Ok(StartupRecoveredSegmentIndexes {
        loaded: finalize_loaded_segment_indexes(parsed, max_segment_id, max_wal_highwater)?,
        quarantined,
    })
}

pub fn read_segment_manifest(path: impl AsRef<Path>) -> Result<SegmentManifest> {
    let root = path.as_ref();
    let layout = SegmentLayout::from_root(root.to_path_buf());
    let manifest_bytes = read_required_segment_file(root, &layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;
    Ok(segment_manifest_from_parsed(&parsed_manifest))
}

pub(crate) fn read_segment_manifest_fingerprint(
    path: impl AsRef<Path>,
) -> Result<SegmentContentFingerprint> {
    let root = path.as_ref();
    let layout = SegmentLayout::from_root(root.to_path_buf());
    let manifest_bytes = read_required_segment_file(root, &layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;
    Ok(segment_fingerprint_from_parsed_manifest(parsed_manifest))
}

pub(crate) fn verify_segment_fingerprint(
    path: impl AsRef<Path>,
) -> Result<SegmentContentFingerprint> {
    let root = path.as_ref();
    let layout = SegmentLayout::from_root(root.to_path_buf());
    let manifest_bytes = read_required_segment_file(root, &layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;

    let chunks_bytes = read_required_segment_file(root, &layout.chunks_path)?;
    let chunk_index_bytes = read_required_segment_file(root, &layout.chunk_index_path)?;
    let series_bytes = read_required_segment_file(root, &layout.series_path)?;
    let postings_bytes = read_required_segment_file(root, &layout.postings_path)?;

    verify_file_manifest_entry(&parsed_manifest.files[0], FILE_KIND_CHUNKS, &chunks_bytes)?;
    verify_file_manifest_entry(
        &parsed_manifest.files[1],
        FILE_KIND_CHUNK_INDEX,
        &chunk_index_bytes,
    )?;
    verify_file_manifest_entry(&parsed_manifest.files[2], FILE_KIND_SERIES, &series_bytes)?;
    verify_file_manifest_entry(
        &parsed_manifest.files[3],
        FILE_KIND_POSTINGS,
        &postings_bytes,
    )?;

    Ok(segment_fingerprint_from_parsed_manifest(parsed_manifest))
}

fn load_segment_indexes_from_dirs_impl(
    dirs: Vec<PathBuf>,
    load_series: bool,
    policy: IndexedSegmentLoadMode,
) -> Result<LoadedSegmentIndexes> {
    let mut parsed = Vec::new();
    let mut max_segment_id = 0u64;
    let mut max_wal_highwater = WalHighWatermark::default();

    for dir in dirs {
        match load_indexed_segment_dir_with_policy(&dir, load_series, policy)? {
            IndexedSegmentLoadOutcome::Loaded(segment) => {
                max_segment_id = max_segment_id.max(segment.manifest.segment_id);
                max_wal_highwater = max_wal_highwater.max(segment.manifest.wal_highwater);
                parsed.push(segment);
            }
            IndexedSegmentLoadOutcome::Skipped => {}
            IndexedSegmentLoadOutcome::Quarantined(_) => {
                return Err(TsinkError::Other(
                    "startup quarantine mode is only valid for startup recovery loads".to_string(),
                ));
            }
        }
    }

    finalize_loaded_segment_indexes(parsed, max_segment_id, max_wal_highwater)
}

fn finalize_loaded_segment_indexes(
    mut parsed: Vec<ParsedIndexedSegment>,
    max_segment_id: u64,
    max_wal_highwater: WalHighWatermark,
) -> Result<LoadedSegmentIndexes> {
    parsed.sort_by_key(|segment| (segment.manifest.level, segment.manifest.segment_id));

    let mut series_by_id = BTreeMap::<SeriesId, PersistedSeries>::new();
    let mut indexed_segments = Vec::with_capacity(parsed.len());

    for segment in parsed {
        merge_persisted_series(&mut series_by_id, segment.series.clone())?;

        indexed_segments.push(IndexedSegment {
            root: segment.root,
            manifest: segment.manifest,
            series: segment.series,
            postings: segment.postings,
            chunk_index: segment.chunk_index,
            chunks_mmap: segment.chunks_mmap,
        });
    }

    Ok(LoadedSegmentIndexes {
        next_segment_id: max_segment_id.saturating_add(1).max(1),
        series: series_by_id.into_values().collect(),
        indexed_segments,
        wal_replay_highwater: max_wal_highwater,
    })
}

pub fn load_segments_for_level(base: impl AsRef<Path>, level: u8) -> Result<Vec<LoadedSegment>> {
    load_segments_for_level_with_policy(base, level, SegmentLoadPolicy::SkipInvalid)
}

fn load_segments_for_level_with_policy(
    base: impl AsRef<Path>,
    level: u8,
    policy: SegmentLoadPolicy,
) -> Result<Vec<LoadedSegment>> {
    let dirs = collect_segment_dirs(base.as_ref(), std::iter::once(level))?;
    let mut out = Vec::new();

    for dir in dirs {
        if let Some(segment) = load_segment_dir_with_policy(
            &dir,
            policy,
            "Segment directory disappeared while loading level; skipping",
            "Ignoring invalid segment directory",
        )? {
            out.push(LoadedSegment {
                root: segment.root,
                manifest: segment.manifest,
                series: segment.series,
                chunks_by_series: segment.chunks_by_series,
            });
        }
    }

    out.sort_by_key(|segment| segment.manifest.segment_id);
    Ok(out)
}

pub fn collect_expired_segment_dirs(
    base: impl AsRef<Path>,
    retention_cutoff: i64,
) -> Result<Vec<PathBuf>> {
    let dirs = collect_segment_dirs(base.as_ref(), 0..=2u8)?;
    let mut expired = Vec::<(u8, u64, PathBuf)>::new();

    for dir in dirs {
        let manifest_path = dir.join("manifest.bin");
        let manifest_bytes = match read_required_segment_file(&dir, &manifest_path) {
            Ok(bytes) => bytes,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %manifest_path.display(),
                    error = %err,
                    "Segment manifest disappeared during retention scan; skipping"
                );
                continue;
            }
            Err(err) => return Err(err),
        };

        let parsed_manifest = match parse_manifest(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(TsinkError::DataCorruption(msg)) => {
                warn!(
                    path = %manifest_path.display(),
                    error = %msg,
                    "Ignoring invalid segment manifest during retention scan"
                );
                continue;
            }
            Err(err) => return Err(err),
        };

        let Some(max_ts) = parsed_manifest.max_ts else {
            continue;
        };

        if max_ts < retention_cutoff {
            expired.push((parsed_manifest.level, parsed_manifest.segment_id, dir));
        }
    }

    expired.sort_by_key(|(level, segment_id, _)| (*level, *segment_id));
    Ok(expired
        .into_iter()
        .map(|(_, _, segment_root)| segment_root)
        .collect())
}

fn collect_segment_dirs(base: &Path, levels: impl IntoIterator<Item = u8>) -> Result<Vec<PathBuf>> {
    collect_segment_dirs_with_limit(
        base,
        levels,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
    )
}

fn collect_segment_dirs_with_limit(
    base: &Path,
    levels: impl IntoIterator<Item = u8>,
    max_entries: usize,
) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(max_entries);
    visit_segment_dirs_with_namespace_budget(base, levels, &mut namespace_budget, |path| {
        dirs.push(path.to_path_buf());
        Ok(())
    })?;
    Ok(dirs)
}

pub(crate) fn visit_segment_dirs_with_namespace_budget(
    base: &Path,
    levels: impl IntoIterator<Item = u8>,
    namespace_budget: &mut crate::engine::fs_utils::RecoveryNamespaceBudget,
    mut visit: impl FnMut(&Path) -> Result<()>,
) -> Result<()> {
    let metadata = match fs::symlink_metadata(base) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!(
                "segment base is link-like or not a directory: {}",
                base.display()
            ),
        )
        .into());
    }

    let segments_root = base.join("segments");
    let metadata = match fs::symlink_metadata(&segments_root) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!(
                "segment root is link-like or not a directory: {}",
                segments_root.display()
            ),
        )
        .into());
    }

    for level in levels {
        let level_root = segments_root.join(format!("L{level}"));
        let metadata = match fs::symlink_metadata(&level_root) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!(
                    "segment level root is link-like or not a directory: {}",
                    level_root.display()
                ),
            )
            .into());
        }
        for entry in fs::read_dir(&level_root).map_err(|source| TsinkError::IoWithPath {
            path: level_root.clone(),
            source,
        })? {
            namespace_budget.observe_entry(&level_root, "segment root discovery")?;
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: level_root.clone(),
                source,
            })?;
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };

            if !file_type.is_dir() {
                continue;
            }

            let path = entry.path();
            if !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("seg-"))
            {
                continue;
            }

            visit(&path)?;
        }
    }
    Ok(())
}

fn load_segment_dir_with_policy(
    dir: &Path,
    policy: SegmentLoadPolicy,
    disappeared_message: &'static str,
    invalid_message: &'static str,
) -> Result<Option<ParsedSegment>> {
    match load_segment_dir(dir) {
        Ok(segment) => Ok(Some(segment)),
        Err(err) => {
            handle_segment_dir_load_error(dir, err, policy, disappeared_message, invalid_message)?;
            Ok(None)
        }
    }
}

fn load_indexed_segment_dir_with_policy(
    dir: &Path,
    load_series: bool,
    policy: IndexedSegmentLoadMode,
) -> Result<IndexedSegmentLoadOutcome> {
    let (metadata_decode_limit_bytes, mapped_file_limit_bytes) = match policy {
        IndexedSegmentLoadMode::Standard(_) => (MAX_DECODED_FRAMED_FILE_BYTES, usize::MAX),
        IndexedSegmentLoadMode::StartupRecoverable {
            metadata_decode_limit_bytes,
        } => (
            metadata_decode_limit_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES),
            metadata_decode_limit_bytes,
        ),
    };
    match load_segment_dir_indexed_with_decoded_limit(
        dir,
        load_series,
        metadata_decode_limit_bytes,
        mapped_file_limit_bytes,
    ) {
        Ok(segment) => Ok(IndexedSegmentLoadOutcome::Loaded(segment)),
        Err(err) => match policy {
            IndexedSegmentLoadMode::Standard(policy) => {
                handle_segment_dir_load_error(
                    dir,
                    err,
                    policy,
                    "Segment directory disappeared during indexed scan; skipping",
                    "Ignoring invalid segment directory",
                )?;
                Ok(IndexedSegmentLoadOutcome::Skipped)
            }
            IndexedSegmentLoadMode::StartupRecoverable { .. } => match err {
                err if is_not_found_error(&err) => {
                    warn!(
                        path = %dir.display(),
                        error = %err,
                        "Segment directory disappeared during indexed startup scan; skipping"
                    );
                    Ok(IndexedSegmentLoadOutcome::Skipped)
                }
                err => {
                    let Some(details) = segment_validation_error_message(&err) else {
                        return Err(err);
                    };
                    Ok(IndexedSegmentLoadOutcome::Quarantined(
                        quarantine_invalid_startup_segment(dir, &details)?,
                    ))
                }
            },
        },
    }
}

fn map_required_segment_file_error(
    segment_root: &Path,
    path: &Path,
    err: std::io::Error,
) -> TsinkError {
    if err.kind() != std::io::ErrorKind::NotFound {
        return err.into();
    }

    match fs::symlink_metadata(segment_root) {
        Ok(metadata) if metadata.is_dir() => {
            let file_name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string());
            TsinkError::DataCorruption(format!("missing {file_name}"))
        }
        Ok(_) => TsinkError::DataCorruption(format!(
            "segment root is not a directory: {}",
            segment_root.display()
        )),
        Err(root_err) if root_err.kind() == std::io::ErrorKind::NotFound => err.into(),
        Err(root_err) => root_err.into(),
    }
}

fn read_required_segment_file(segment_root: &Path, path: &Path) -> Result<Vec<u8>> {
    read_required_segment_file_with_runtime_limit(segment_root, path, usize::MAX)
}

fn read_required_segment_file_with_runtime_limit(
    segment_root: &Path,
    path: &Path,
    runtime_limit_bytes: usize,
) -> Result<Vec<u8>> {
    let format_max_bytes = match path.file_name().and_then(|name| name.to_str()) {
        Some("chunks.bin") => MAX_SEGMENT_CHUNKS_FILE_BYTES,
        Some("manifest.bin") => MAX_SEGMENT_MANIFEST_FILE_BYTES,
        _ => MAX_DECODED_FRAMED_FILE_BYTES,
    };
    let mut file =
        File::open(path).map_err(|err| map_required_segment_file_error(segment_root, path, err))?;
    let declared_len = file
        .metadata()
        .map_err(|err| map_required_segment_file_error(segment_root, path, err))?
        .len();
    if declared_len > format_max_bytes as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "segment file {} size {declared_len} exceeds the format safety limit {format_max_bytes}",
            path.display()
        )));
    }
    let active_max_bytes = format_max_bytes.min(runtime_limit_bytes);
    if declared_len > u64::try_from(active_max_bytes).unwrap_or(u64::MAX) {
        return Err(TsinkError::MemoryBudgetExceeded {
            budget: runtime_limit_bytes,
            required: usize::try_from(declared_len).unwrap_or(usize::MAX),
        });
    }

    let initial_capacity = usize::try_from(declared_len)
        .unwrap_or(active_max_bytes)
        .min(active_max_bytes);
    match read_to_end_bounded(
        &mut file,
        active_max_bytes,
        initial_capacity,
        &format!("segment file {}", path.display()),
    ) {
        Ok(bytes) => Ok(bytes),
        Err(TsinkError::Io(err)) => Err(map_required_segment_file_error(segment_root, path, err)),
        Err(err) => Err(err),
    }
}

fn open_required_segment_file_with_runtime_limit(
    segment_root: &Path,
    path: &Path,
    runtime_limit_bytes: usize,
) -> Result<File> {
    let file =
        File::open(path).map_err(|err| map_required_segment_file_error(segment_root, path, err))?;
    if path.file_name().and_then(|name| name.to_str()) == Some("chunks.bin") {
        let declared_len = file
            .metadata()
            .map_err(|err| map_required_segment_file_error(segment_root, path, err))?
            .len();
        if declared_len > MAX_SEGMENT_CHUNKS_FILE_BYTES as u64 {
            return Err(TsinkError::DataCorruption(format!(
                "segment file {} size {declared_len} exceeds the format safety limit {MAX_SEGMENT_CHUNKS_FILE_BYTES}",
                path.display()
            )));
        }
        if declared_len > u64::try_from(runtime_limit_bytes).unwrap_or(u64::MAX) {
            return Err(TsinkError::MemoryBudgetExceeded {
                budget: runtime_limit_bytes,
                required: usize::try_from(declared_len).unwrap_or(usize::MAX),
            });
        }
    }
    Ok(file)
}

fn enforce_framed_metadata_runtime_limit(
    bytes: &[u8],
    expected_magic: [u8; 4],
    file_name: &str,
    runtime_limit_bytes: usize,
) -> Result<()> {
    if bytes.len() < 8
        || bytes[..4] != expected_magic
        || u16::from_le_bytes([bytes[4], bytes[5]]) != FORMAT_VERSION
        || u16::from_le_bytes([bytes[6], bytes[7]]) & FILE_FLAG_ZSTD_BODY == 0
        || bytes.len() < 12
    {
        return Ok(());
    }
    let body_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let decoded_bytes = 8usize.checked_add(body_len).ok_or_else(|| {
        TsinkError::DataCorruption("segment metadata decoded length overflow".to_string())
    })?;
    if decoded_bytes > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "{file_name} decoded size {decoded_bytes} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    if decoded_bytes > runtime_limit_bytes {
        return Err(TsinkError::MemoryBudgetExceeded {
            budget: runtime_limit_bytes,
            required: decoded_bytes,
        });
    }
    Ok(())
}

fn load_segment_dir(path: &Path) -> Result<ParsedSegment> {
    let layout = SegmentLayout::from_root(path.to_path_buf());

    let manifest_bytes = read_required_segment_file(path, &layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;
    drop(manifest_bytes);

    let series_bytes = read_required_segment_file(path, &layout.series_path)?;
    verify_file_manifest_entry(&parsed_manifest.files[2], FILE_KIND_SERIES, &series_bytes)?;
    let parsed_series = parse_series_file(&series_bytes)?;
    drop(series_bytes);

    let postings_bytes = read_required_segment_file(path, &layout.postings_path)?;
    verify_file_manifest_entry(
        &parsed_manifest.files[3],
        FILE_KIND_POSTINGS,
        &postings_bytes,
    )?;
    drop(parse_postings_file(&postings_bytes, &parsed_series)?);
    drop(postings_bytes);
    let series = decode_persisted_series(&parsed_series)?;
    drop(parsed_series);
    let series_value_families = series
        .iter()
        .map(|series| (series.series_id, series.value_family))
        .collect::<HashMap<_, _>>();

    let chunk_index_bytes = read_required_segment_file(path, &layout.chunk_index_path)?;
    verify_file_manifest_entry(
        &parsed_manifest.files[1],
        FILE_KIND_CHUNK_INDEX,
        &chunk_index_bytes,
    )?;
    let chunk_index = parse_chunk_index_file(&chunk_index_bytes)?;
    drop(chunk_index_bytes);

    let chunks_bytes = read_required_segment_file(path, &layout.chunks_path)?;
    verify_file_manifest_entry(&parsed_manifest.files[0], FILE_KIND_CHUNKS, &chunks_bytes)?;
    let chunk_records = parse_chunks_file(&chunks_bytes)?;
    drop(chunks_bytes);
    let chunks_by_series =
        rebuild_chunks_by_series(&chunk_index, chunk_records, &series_value_families)?;

    Ok(ParsedSegment {
        root: path.to_path_buf(),
        manifest: segment_manifest_from_parsed(&parsed_manifest),
        series,
        chunks_by_series,
    })
}

fn load_segment_dir_indexed_with_decoded_limit(
    path: &Path,
    load_series: bool,
    metadata_decode_limit_bytes: usize,
    mapped_file_limit_bytes: usize,
) -> Result<ParsedIndexedSegment> {
    let layout = SegmentLayout::from_root(path.to_path_buf());

    let manifest_bytes = read_required_segment_file(path, &layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;
    drop(manifest_bytes);
    let chunk_index_bytes = read_required_segment_file_with_runtime_limit(
        path,
        &layout.chunk_index_path,
        metadata_decode_limit_bytes,
    )?;
    enforce_framed_metadata_runtime_limit(
        &chunk_index_bytes,
        CHUNK_INDEX_MAGIC,
        "chunk_index.bin",
        metadata_decode_limit_bytes,
    )?;

    let chunks_file = open_required_segment_file_with_runtime_limit(
        path,
        &layout.chunks_path,
        mapped_file_limit_bytes,
    )?;
    let chunks_mmap = create_mmap(chunks_file).map_err(|err| TsinkError::MemoryMap {
        path: layout.chunks_path.clone(),
        details: err.to_string(),
    })?;

    verify_file_manifest_entry(
        &parsed_manifest.files[0],
        FILE_KIND_CHUNKS,
        chunks_mmap.as_slice(),
    )?;
    verify_file_manifest_entry(
        &parsed_manifest.files[1],
        FILE_KIND_CHUNK_INDEX,
        &chunk_index_bytes,
    )?;
    let chunk_index =
        parse_chunk_index_file_with_decoded_limit(&chunk_index_bytes, metadata_decode_limit_bytes)?;
    drop(chunk_index_bytes);
    validate_chunk_index_against_chunks_file(chunks_mmap.as_slice(), &chunk_index)?;

    let mut postings = SegmentPostingsIndex::default();
    let series = if load_series {
        let series_bytes = read_required_segment_file_with_runtime_limit(
            path,
            &layout.series_path,
            metadata_decode_limit_bytes,
        )?;
        enforce_framed_metadata_runtime_limit(
            &series_bytes,
            SERIES_MAGIC,
            "series.bin",
            metadata_decode_limit_bytes,
        )?;
        verify_file_manifest_entry(&parsed_manifest.files[2], FILE_KIND_SERIES, &series_bytes)?;
        let parsed_series =
            parse_series_file_with_decoded_limit(&series_bytes, metadata_decode_limit_bytes)?;
        drop(series_bytes);

        let postings_bytes = read_required_segment_file_with_runtime_limit(
            path,
            &layout.postings_path,
            metadata_decode_limit_bytes,
        )?;
        enforce_framed_metadata_runtime_limit(
            &postings_bytes,
            POSTINGS_MAGIC,
            "postings.bin",
            metadata_decode_limit_bytes,
        )?;
        verify_file_manifest_entry(
            &parsed_manifest.files[3],
            FILE_KIND_POSTINGS,
            &postings_bytes,
        )?;
        postings = parse_postings_file_with_decoded_limit(
            &postings_bytes,
            &parsed_series,
            metadata_decode_limit_bytes,
        )?;
        drop(postings_bytes);
        decode_persisted_series(&parsed_series)?
    } else {
        Vec::new()
    };

    if !load_series {
        postings.series_postings = chunk_index
            .entries
            .iter()
            .map(|entry| entry.series_id)
            .collect();
    }

    Ok(ParsedIndexedSegment {
        root: path.to_path_buf(),
        manifest: segment_manifest_from_parsed(&parsed_manifest),
        series,
        postings,
        chunk_index,
        chunks_mmap,
    })
}

fn load_segment_dir_indexed(path: &Path, load_series: bool) -> Result<ParsedIndexedSegment> {
    load_segment_dir_indexed_with_decoded_limit(
        path,
        load_series,
        MAX_DECODED_FRAMED_FILE_BYTES,
        usize::MAX,
    )
}

fn rebuild_chunks_by_series(
    chunk_index: &ChunkIndex,
    mut chunk_records: BTreeMap<u64, ChunkRecordMeta>,
    series_value_families: &HashMap<SeriesId, Option<crate::engine::series::SeriesValueFamily>>,
) -> Result<HashMap<SeriesId, Vec<Chunk>>> {
    let mut chunks_by_series = HashMap::<SeriesId, Vec<Chunk>>::new();

    for entry in &chunk_index.entries {
        let Some(meta) = chunk_records.remove(&entry.chunk_offset) else {
            return Err(TsinkError::DataCorruption(format!(
                "chunk index references missing chunk offset {}",
                entry.chunk_offset
            )));
        };

        if meta.len != entry.chunk_len {
            return Err(TsinkError::DataCorruption(format!(
                "chunk length mismatch at offset {}: index {}, chunk {}",
                entry.chunk_offset, entry.chunk_len, meta.len
            )));
        }

        if meta.chunk.header.series_id != entry.series_id
            || meta.chunk.header.min_ts != entry.min_ts
            || meta.chunk.header.max_ts != entry.max_ts
            || meta.chunk.header.point_count != entry.point_count
            || meta.chunk.header.lane != entry.lane
            || meta.chunk.header.ts_codec != entry.ts_codec
            || meta.chunk.header.value_codec != entry.value_codec
        {
            return Err(TsinkError::DataCorruption(
                "chunk index entry does not match chunk header".to_string(),
            ));
        }

        let Some(value_family) = series_value_families.get(&entry.series_id).copied() else {
            return Err(TsinkError::DataCorruption(format!(
                "chunk index references series id {} missing from series metadata",
                entry.series_id
            )));
        };

        let mut chunk = meta.chunk;
        chunk.header.value_family = value_family;
        chunks_by_series
            .entry(entry.series_id)
            .or_default()
            .push(chunk);
    }

    if let Some(unindexed_offset) = chunk_records.keys().next().copied() {
        return Err(TsinkError::DataCorruption(format!(
            "chunks.bin contains unindexed chunk record at offset {unindexed_offset}"
        )));
    }

    for chunks in chunks_by_series.values_mut() {
        sort_chunks_by_time(chunks);
    }

    Ok(chunks_by_series)
}

fn merge_persisted_series(
    series_by_id: &mut BTreeMap<SeriesId, PersistedSeries>,
    incoming: Vec<PersistedSeries>,
) -> Result<()> {
    for series in incoming {
        match series_by_id.get(&series.series_id) {
            Some(existing)
                if existing.metric == series.metric && existing.labels == series.labels => {}
            Some(_) => {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} conflicts across segments",
                    series.series_id
                )));
            }
            None => {
                series_by_id.insert(series.series_id, series);
            }
        }
    }

    Ok(())
}

fn sort_chunks_by_time(chunks: &mut [Chunk]) {
    chunks.sort_by(|a, b| {
        (a.header.min_ts, a.header.max_ts, a.header.point_count).cmp(&(
            b.header.min_ts,
            b.header.max_ts,
            b.header.point_count,
        ))
    });
}

fn segment_fingerprint_from_parsed_manifest(
    parsed_manifest: super::format::ParsedManifest,
) -> SegmentContentFingerprint {
    SegmentContentFingerprint {
        manifest: segment_manifest_from_parsed(&parsed_manifest),
        files: parsed_manifest.files.map(|entry| SegmentFileFingerprint {
            kind: entry.kind,
            file_len: entry.file_len,
            hash64: entry.hash64,
        }),
    }
}

#[cfg(test)]
mod namespace_bound_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn segment_root_discovery_uses_one_cap_across_levels_and_unknown_entries() {
        let temp_dir = TempDir::new().unwrap();
        let level_zero = temp_dir.path().join("segments/L0");
        let level_one = temp_dir.path().join("segments/L1");
        fs::create_dir_all(level_zero.join("seg-0000000000000001")).unwrap();
        fs::create_dir_all(&level_one).unwrap();
        fs::write(level_one.join("host-owned"), b"opaque").unwrap();

        let dirs = collect_segment_dirs_with_limit(temp_dir.path(), [0, 1], 2)
            .expect("the exact cap across two levels must succeed");
        assert_eq!(dirs, vec![level_zero.join("seg-0000000000000001")]);

        fs::create_dir(level_one.join("unknown-directory")).unwrap();
        let err = collect_segment_dirs_with_limit(temp_dir.path(), [0, 1], 2)
            .expect_err("cap plus one in a later level must fail");
        assert!(err.to_string().contains("2-entry global work bound"));
    }

    #[test]
    fn bounded_segment_file_read_accepts_n_and_rejects_n_plus_one_from_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = temp_dir.path().join("manifest.bin");
        File::create(&manifest)
            .unwrap()
            .set_len(MAX_SEGMENT_MANIFEST_FILE_BYTES as u64)
            .unwrap();
        assert_eq!(
            read_required_segment_file(temp_dir.path(), &manifest)
                .unwrap()
                .len(),
            MAX_SEGMENT_MANIFEST_FILE_BYTES
        );

        File::options()
            .write(true)
            .open(&manifest)
            .unwrap()
            .set_len(MAX_SEGMENT_MANIFEST_FILE_BYTES as u64 + 1)
            .unwrap();
        let err = read_required_segment_file(temp_dir.path(), &manifest).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds the format safety limit")));
    }

    #[test]
    fn startup_metadata_runtime_limit_is_exact_and_preserves_format_validation() {
        let mut framed = Vec::from(CHUNK_INDEX_MAGIC);
        framed.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        framed.extend_from_slice(&FILE_FLAG_ZSTD_BODY.to_le_bytes());
        framed.extend_from_slice(&56u32.to_le_bytes());

        enforce_framed_metadata_runtime_limit(&framed, CHUNK_INDEX_MAGIC, "chunk_index.bin", 64)
            .expect("the exact decoded runtime limit must be admitted");
        assert!(matches!(
            enforce_framed_metadata_runtime_limit(
                &framed,
                CHUNK_INDEX_MAGIC,
                "chunk_index.bin",
                63
            ),
            Err(TsinkError::MemoryBudgetExceeded {
                budget: 63,
                required: 64
            })
        ));

        framed[8..12].copy_from_slice(&(MAX_DECODED_FRAMED_FILE_BYTES as u32).to_le_bytes());
        let err = enforce_framed_metadata_runtime_limit(
            &framed,
            CHUNK_INDEX_MAGIC,
            "chunk_index.bin",
            63,
        )
        .expect_err("the format ceiling must take precedence over the runtime ceiling");
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("format safety limit")));

        framed[..4].copy_from_slice(b"BAD!");
        enforce_framed_metadata_runtime_limit(&framed, CHUNK_INDEX_MAGIC, "chunk_index.bin", 63)
            .expect("invalid framing is left for the format parser, not classified as memory use");
    }

    #[test]
    fn startup_chunk_mapping_limit_does_not_lower_the_standard_format_ceiling() {
        let temp_dir = TempDir::new().unwrap();
        let chunks = temp_dir.path().join("chunks.bin");
        let above_metadata_ceiling = MAX_DECODED_FRAMED_FILE_BYTES + 1;
        File::create(&chunks)
            .unwrap()
            .set_len(above_metadata_ceiling as u64)
            .unwrap();

        open_required_segment_file_with_runtime_limit(temp_dir.path(), &chunks, usize::MAX)
            .expect("standard indexed loads retain the separate 1 GiB chunks.bin ceiling");
        assert!(matches!(
            open_required_segment_file_with_runtime_limit(
                temp_dir.path(),
                &chunks,
                MAX_DECODED_FRAMED_FILE_BYTES
            ),
            Err(TsinkError::MemoryBudgetExceeded { budget, required })
                if budget == MAX_DECODED_FRAMED_FILE_BYTES
                    && required == above_metadata_ceiling
        ));
    }
}
