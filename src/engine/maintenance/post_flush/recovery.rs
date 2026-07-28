use std::collections::BTreeSet;
use std::fs::{self, ReadDir};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::engine::segment::SegmentValidationContext;

use super::super::super::tiering::{
    PersistedSegmentTier, SegmentInventoryEntry, SegmentLaneFamily, SegmentPathResolver,
};
use super::super::super::*;
use super::super::{RemoteCatalogMemoryReservation, RetiredPostFlushRoot, StagedSegmentPromotion};

pub(in crate::engine::storage_engine) const POST_FLUSH_REPLACEMENT_DIR_NAME: &str =
    ".post-flush-replacements";
const POST_FLUSH_REPLACEMENT_VERSION: u16 = 1;
const POST_FLUSH_REPLACEMENT_MARKER_PREFIX: &str = "transaction-";
const POST_FLUSH_REPLACEMENT_MARKER_SUFFIX: &str = ".json";
const MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES: u64 = 4 * 1024 * 1024;
pub(super) const MAX_POST_FLUSH_REPLACEMENT_RECORDS: usize = 16 * 1024;
const BOUNDED_POST_FLUSH_MARKER_CURSOR_BASE_BYTES: usize = 4 * 1024;
const BOUNDED_POST_FLUSH_MARKER_PARSE_BASE_BYTES: usize = 16 * 1024;
const BOUNDED_POST_FLUSH_MARKER_PARSE_PAYLOAD_COPIES: usize = 6;
const BOUNDED_POST_FLUSH_TRANSITION_RECORD_BYTES: usize = 1024;
const BOUNDED_POST_FLUSH_TRANSITION_PATH_COPIES: usize = 16;
const BOUNDED_POST_FLUSH_RECOVERY_OPERATION: &str = "bounded post-flush replacement recovery";

static POST_FLUSH_REPLACEMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PostFlushReplacementPhase {
    Prepared,
    Committing,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct PostFlushSegmentRecord {
    lane: SegmentLaneFamily,
    tier: PersistedSegmentTier,
    relative_path: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PostFlushSourceRecord {
    segment: PostFlushSegmentRecord,
    counts_as_expired: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PostFlushReplacementMarker {
    version: u16,
    phase: PostFlushReplacementPhase,
    id: String,
    sources: Vec<PostFlushSourceRecord>,
    outputs: Vec<PostFlushSegmentRecord>,
    tier_moves: u64,
}

#[derive(Clone, Debug)]
struct ValidatedSegmentRecord {
    record: PostFlushSegmentRecord,
    root: PathBuf,
    level: u8,
    segment_id: u64,
}

#[derive(Clone, Debug)]
struct ValidatedSourceRecord {
    segment: ValidatedSegmentRecord,
    counts_as_expired: bool,
}

#[derive(Clone, Debug)]
pub(super) struct PostFlushReplacement {
    marker_path: PathBuf,
    marker: PostFlushReplacementMarker,
    sources: Vec<ValidatedSourceRecord>,
    outputs: Vec<ValidatedSegmentRecord>,
}

struct BackgroundPostFlushMarkerScan {
    marker_dir: PathBuf,
    entries: ReadDir,
    observed_entries: usize,
    _memory_reservation: RemoteCatalogMemoryReservation,
}

/// Process-local continuation for the finite runtime marker namespace scan.
///
/// The retained `ReadDir` ensures that one background wake never has to collect or sort the
/// complete marker namespace. Startup and foreground lifecycle drains deliberately keep their
/// complete, strict scan because they are not finite maintenance pages.
#[derive(Default)]
pub(in crate::engine::storage_engine) struct BackgroundPostFlushRecoveryCursor {
    scan: Option<BackgroundPostFlushMarkerScan>,
}

pub(super) struct BoundedRuntimeReplacement {
    replacement: PostFlushReplacement,
    source_states: Vec<OwnedSegmentState>,
    output_entries: Vec<SegmentInventoryEntry>,
    source_roots: Vec<PathBuf>,
    selected_items: usize,
    selected_bytes: u64,
    retained_memory_bytes: usize,
    _marker_memory_reservation: RemoteCatalogMemoryReservation,
    _segment_memory_reservation: RemoteCatalogMemoryReservation,
}

pub(super) enum BoundedRuntimeReplacementStep {
    NoPending,
    NamespaceEntryConsumed,
    PreparedRolledBack,
    Committing(Box<BoundedRuntimeReplacement>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnedSegmentState {
    Live,
    Retired,
    Gone,
}

fn replacement_marker_dir(data_path: &Path) -> PathBuf {
    data_path.join(POST_FLUSH_REPLACEMENT_DIR_NAME)
}

pub(in crate::engine::storage_engine) fn is_post_flush_replacement_marker_name(name: &str) -> bool {
    marker_id_from_name(name).is_some()
}

fn marker_id_from_name(name: &str) -> Option<&str> {
    let body = name
        .strip_prefix(POST_FLUSH_REPLACEMENT_MARKER_PREFIX)?
        .strip_suffix(POST_FLUSH_REPLACEMENT_MARKER_SUFFIX)?;
    let (timestamp, nonce) = body.split_once('-')?;
    if is_exact_lower_hex(timestamp, 16) && is_exact_lower_hex(nonce, 16) {
        Some(body)
    } else {
        None
    }
}

fn is_exact_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_relative_segment_path(path: &str) -> Result<(u8, u64)> {
    let components = Path::new(path).components().collect::<Vec<_>>();
    let [Component::Normal(segments), Component::Normal(level), Component::Normal(segment)] =
        components.as_slice()
    else {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement path must be an exact segment root: {path}"
        )));
    };
    if *segments != std::ffi::OsStr::new("segments") {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement path is outside the segment namespace: {path}"
        )));
    }
    let level_name = level.to_str().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "post-flush replacement level is not valid UTF-8: {path}"
        ))
    })?;
    let level = level_name
        .strip_prefix('L')
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|value| *value <= 2 && level_name == format!("L{value}"))
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "post-flush replacement path has an invalid level: {path}"
            ))
        })?;
    let segment_name = segment.to_str().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "post-flush replacement segment name is not valid UTF-8: {path}"
        ))
    })?;
    let segment_hex = segment_name.strip_prefix("seg-").ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "post-flush replacement path has an invalid segment name: {path}"
        ))
    })?;
    if !is_exact_lower_hex(segment_hex, 16) {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement path has an invalid segment id: {path}"
        )));
    }
    let segment_id = u64::from_str_radix(segment_hex, 16).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "post-flush replacement path has an invalid segment id: {path}"
        ))
    })?;
    Ok((level, segment_id))
}

fn canonical_relative_segment_path(level: u8, segment_id: u64) -> String {
    format!("segments/L{level}/seg-{segment_id:016x}")
}

fn validate_segment_path_components_no_follow(lane_root: &Path, relative_path: &str) -> Result<()> {
    let lane_metadata =
        fs::symlink_metadata(lane_root).map_err(|source| TsinkError::IoWithPath {
            path: lane_root.to_path_buf(),
            source,
        })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&lane_metadata)
        || !lane_metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush configured lane/tier root is link-like or not a directory: {}",
            lane_root.display()
        )));
    }
    // The configured path may itself sit below an intentionally symlinked data-path prefix. Use
    // its canonical directory as the containment boundary, then reject every link-like segment
    // namespace component below that exact configured root.
    let canonical_root = fs::canonicalize(lane_root).map_err(|source| TsinkError::IoWithPath {
        path: lane_root.to_path_buf(),
        source,
    })?;
    let components = Path::new(relative_path).components().collect::<Vec<_>>();
    let mut resolved = canonical_root.clone();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(component) = component else {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement path contains a non-normal component: {relative_path}"
            )));
        };
        resolved.push(component);
        match fs::symlink_metadata(&resolved) {
            Ok(metadata)
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_dir() =>
            {
                return Err(TsinkError::DataCorruption(format!(
                    "post-flush replacement path contains a link-like or non-directory component: {}",
                    resolved.display()
                )))
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                for tail in components.iter().skip(index + 1) {
                    let Component::Normal(tail) = tail else {
                        return Err(TsinkError::DataCorruption(format!(
                            "post-flush replacement path contains a non-normal component: {relative_path}"
                        )));
                    };
                    resolved.push(tail);
                }
                break;
            }
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: resolved,
                    source,
                })
            }
        }
    }
    if resolved == canonical_root || !resolved.starts_with(&canonical_root) {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement path resolves outside configured root {}: {relative_path}",
            lane_root.display()
        )));
    }
    Ok(())
}

fn configured_segment_roots(
    resolver: SegmentPathResolver<'_>,
) -> Vec<(SegmentLaneFamily, PersistedSegmentTier, PathBuf)> {
    let mut roots = Vec::new();
    for lane in [SegmentLaneFamily::Numeric, SegmentLaneFamily::Blob] {
        for tier in [
            PersistedSegmentTier::Hot,
            PersistedSegmentTier::Warm,
            PersistedSegmentTier::Cold,
        ] {
            if let Ok(root) = resolver.lane_root(lane, tier) {
                roots.push((lane, tier, root));
            }
        }
    }
    roots
}

fn locate_segment_record(
    resolver: SegmentPathResolver<'_>,
    root: &Path,
) -> Result<ValidatedSegmentRecord> {
    let mut matches = Vec::new();
    for (lane, tier, lane_root) in configured_segment_roots(resolver) {
        let Ok(relative) = root.strip_prefix(&lane_root) else {
            continue;
        };
        let Some(relative) = relative.to_str() else {
            continue;
        };
        let Ok((level, segment_id)) = parse_relative_segment_path(relative) else {
            continue;
        };
        let canonical = canonical_relative_segment_path(level, segment_id);
        if root != lane_root.join(&canonical) {
            continue;
        }
        validate_segment_path_components_no_follow(&lane_root, &canonical)?;
        matches.push(ValidatedSegmentRecord {
            record: PostFlushSegmentRecord {
                lane,
                tier,
                relative_path: canonical,
            },
            root: root.to_path_buf(),
            level,
            segment_id,
        });
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(TsinkError::InvalidConfiguration(format!(
            "post-flush replacement segment is outside every configured lane/tier root or is noncanonical: {}",
            root.display()
        ))),
        _ => Err(TsinkError::InvalidConfiguration(format!(
            "post-flush replacement segment ambiguously matches multiple configured lane/tier roots: {}",
            root.display()
        ))),
    }
}

fn resolve_segment_record(
    resolver: SegmentPathResolver<'_>,
    record: &PostFlushSegmentRecord,
) -> Result<ValidatedSegmentRecord> {
    let (level, segment_id) = parse_relative_segment_path(&record.relative_path)?;
    if record.relative_path != canonical_relative_segment_path(level, segment_id) {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement segment path is not canonical: {}",
            record.relative_path
        )));
    }
    let lane_root = resolver
        .lane_root(record.lane, record.tier)
        .map_err(|err| {
            TsinkError::DataCorruption(format!(
                "post-flush replacement references an unconfigured lane/tier root: {err}"
            ))
        })?;
    validate_segment_path_components_no_follow(&lane_root, &record.relative_path)?;
    Ok(ValidatedSegmentRecord {
        record: record.clone(),
        root: lane_root.join(&record.relative_path),
        level,
        segment_id,
    })
}

fn validate_complete_segment(
    segment: &ValidatedSegmentRecord,
    actual_root: &Path,
) -> Result<crate::engine::segment::LoadedSegment> {
    crate::engine::segment::load_complete_segment_no_follow(
        actual_root,
        segment.level,
        segment.segment_id,
        SegmentValidationContext::Maintenance,
    )
}

fn entry_metadata(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn require_absent(path: &Path, description: &str) -> Result<()> {
    if entry_metadata(path)?.is_some() {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement {description} already exists: {}",
            path.display()
        )));
    }
    Ok(())
}

fn owned_sibling_path(
    segment_root: &Path,
    marker_id: &str,
    index: usize,
    purpose: &str,
) -> Result<PathBuf> {
    let parent = segment_root.parent().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "post-flush replacement segment has no parent: {}",
            segment_root.display()
        ))
    })?;
    Ok(parent.join(format!(
        ".tsink-post-flush-{purpose}-{marker_id}-{index:04x}"
    )))
}

fn source_retirement_path(replacement: &PostFlushReplacement, index: usize) -> Result<PathBuf> {
    owned_sibling_path(
        &replacement.sources[index].segment.root,
        &replacement.marker.id,
        index,
        "retired",
    )
}

fn output_rollback_path(replacement: &PostFlushReplacement, index: usize) -> Result<PathBuf> {
    owned_sibling_path(
        &replacement.outputs[index].root,
        &replacement.marker.id,
        index,
        "rollback",
    )
}

fn allocate_marker(data_path: &Path) -> Result<(String, PathBuf)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    for _ in 0..256 {
        let nonce = POST_FLUSH_REPLACEMENT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id = format!("{timestamp:016x}-{nonce:016x}");
        let path = replacement_marker_dir(data_path).join(format!(
            "{POST_FLUSH_REPLACEMENT_MARKER_PREFIX}{id}{POST_FLUSH_REPLACEMENT_MARKER_SUFFIX}"
        ));
        match fs::symlink_metadata(&path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((id, path)),
            Ok(_) => continue,
            Err(source) => return Err(TsinkError::IoWithPath { path, source }),
        }
    }
    Err(TsinkError::Other(
        "failed to allocate an unused post-flush replacement marker name".to_string(),
    ))
}

fn ensure_marker_dir(data_path: &Path) -> Result<()> {
    let marker_dir = replacement_marker_dir(data_path);
    crate::engine::fs_utils::create_dir_all_and_sync_parents(&marker_dir)?;
    let metadata = fs::symlink_metadata(&marker_dir).map_err(|source| TsinkError::IoWithPath {
        path: marker_dir.clone(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker root is link-like or not a directory: {}",
            marker_dir.display()
        )));
    }
    Ok(())
}

fn write_marker_and_resolve_ambiguity(
    data_path: &Path,
    resolver: SegmentPathResolver<'_>,
    marker_path: &Path,
    marker: &PostFlushReplacementMarker,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let record_count = marker
        .sources
        .len()
        .checked_add(marker.outputs.len())
        .ok_or_else(|| {
            TsinkError::Other("post-flush marker record count overflowed".to_string())
        })?;
    if record_count > MAX_POST_FLUSH_REPLACEMENT_RECORDS {
        return Err(TsinkError::InvalidConfiguration(format!(
            "post-flush replacement marker has {record_count} records, exceeding the {} record limit",
            MAX_POST_FLUSH_REPLACEMENT_RECORDS
        )));
    }
    if usize::try_from(marker.tier_moves)
        .ok()
        .is_none_or(|tier_moves| tier_moves > marker.outputs.len())
    {
        return Err(TsinkError::InvalidConfiguration(
            "post-flush replacement tier-move count exceeds its output count".to_string(),
        ));
    }
    let payload = serde_json::to_vec(marker)?;
    if payload.len() as u64 > MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "post-flush replacement marker exceeds the {} byte limit",
            MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES
        )));
    }
    let write_and_resolve = || {
        ensure_marker_dir(data_path)?;
        let write_result =
            crate::engine::fs_utils::write_file_atomically_and_sync_parent(marker_path, &payload);
        let Err(write_error) = write_result else {
            return Ok(());
        };

        // A visible phase is not yet permission to mutate catalog/source state after a failed
        // write. First make the marker directory durable; if that retry fails, leave every path
        // and marker untouched for restart to interpret from the phase that actually survived.
        crate::engine::fs_utils::sync_parent_dir(marker_path).map_err(|sync_error| {
            TsinkError::Other(format!(
                "post-flush marker write failed: {write_error}; marker parent durability retry failed: {sync_error}"
            ))
        })?;
        let visible = parse_marker(data_path, resolver, marker_path).map_err(|probe_error| {
            TsinkError::Other(format!(
                "post-flush marker write failed: {write_error}; durable visible marker validation failed: {probe_error}"
            ))
        })?;
        if visible.marker == *marker {
            Ok(())
        } else {
            Err(TsinkError::Other(format!(
                "post-flush marker write failed and the durable visible marker does not match the intended phase: {write_error}"
            )))
        }
    };

    let Some(budget) = local_disk_budget else {
        return write_and_resolve();
    };
    if !budget.governs_entry(marker_path)? {
        return write_and_resolve();
    }
    let missing_directories = if entry_metadata(marker_path)?.is_none() {
        // The helper counts missing parent directories. Passing the final marker path therefore
        // includes a missing `.post-flush-replacements` entry, while the +1 below is the atomic
        // marker temporary/final entry itself.
        budget.missing_managed_parent_directory_count(std::slice::from_ref(
            &marker_path.to_path_buf(),
        ))?
    } else {
        0
    };
    let entry_count = missing_directories.checked_add(1).ok_or_else(|| {
        TsinkError::Other(
            "post-flush marker entry allowance exceeds the supported range".to_string(),
        )
    })?;
    let entry_allowance = entry_count
        .checked_mul(budget.snapshot_restore_entry_staging_allowance_bytes()?)
        .ok_or_else(|| {
            TsinkError::Other(
                "post-flush marker filesystem allowance exceeds the supported range".to_string(),
            )
        })?;
    let peak_bytes = u64::try_from(payload.len())
        .ok()
        .and_then(|payload_bytes| payload_bytes.checked_add(entry_allowance))
        .ok_or_else(|| {
            TsinkError::Other(
                "post-flush marker publication peak exceeds the supported range".to_string(),
            )
        })?;
    budget.with_reconciled_maintenance_reservation(
        crate::DiskCategory::Temporary,
        peak_bytes,
        write_and_resolve,
    )
}

fn parse_marker(
    _data_path: &Path,
    resolver: SegmentPathResolver<'_>,
    marker_path: &Path,
) -> Result<PostFlushReplacement> {
    let metadata = fs::symlink_metadata(marker_path).map_err(|source| TsinkError::IoWithPath {
        path: marker_path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker is link-like or not a regular file: {}",
            marker_path.display()
        )));
    }
    let file_name = marker_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "post-flush replacement marker has a non-UTF-8 name: {}",
                marker_path.display()
            ))
        })?;
    let marker_id = marker_id_from_name(file_name).ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "post-flush replacement marker has an invalid name: {}",
            marker_path.display()
        ))
    })?;
    if metadata.len() > MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker exceeds the {} byte limit: {}",
            MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES,
            marker_path.display()
        )));
    }
    let mut file = regular_file_read_options()
        .open(marker_path)
        .map_err(|source| TsinkError::IoWithPath {
            path: marker_path.to_path_buf(),
            source,
        })?;
    let opened_metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: marker_path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&opened_metadata)
        || !opened_metadata.file_type().is_file()
        || opened_metadata.len() != metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker changed before its opened handle was validated: {}",
            marker_path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.by_ref()
        .take(MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| TsinkError::IoWithPath {
            path: marker_path.to_path_buf(),
            source,
        })?;
    if bytes.len() as u64 > MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES
        || bytes.len() as u64 != metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker changed size or exceeded its byte limit while reading: {}",
            marker_path.display()
        )));
    }
    let after_metadata =
        fs::symlink_metadata(marker_path).map_err(|source| TsinkError::IoWithPath {
            path: marker_path.to_path_buf(),
            source,
        })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&after_metadata)
        || !after_metadata.file_type().is_file()
        || after_metadata.len() != metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker changed entry type or size while reading: {}",
            marker_path.display()
        )));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let marker = PostFlushReplacementMarker::deserialize(&mut deserializer)?;
    deserializer.end()?;
    if marker.version != POST_FLUSH_REPLACEMENT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported post-flush replacement marker version {}",
            marker.version
        )));
    }
    if marker.id != marker_id {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker id does not match its file name: {}",
            marker_path.display()
        )));
    }
    if marker.sources.is_empty() {
        return Err(TsinkError::DataCorruption(
            "post-flush replacement marker has no sources".to_string(),
        ));
    }
    let record_count = marker
        .sources
        .len()
        .checked_add(marker.outputs.len())
        .ok_or_else(|| {
            TsinkError::DataCorruption(
                "post-flush replacement marker record count overflowed".to_string(),
            )
        })?;
    if record_count > MAX_POST_FLUSH_REPLACEMENT_RECORDS {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker has {record_count} records, exceeding the {} record limit",
            MAX_POST_FLUSH_REPLACEMENT_RECORDS
        )));
    }
    usize::try_from(marker.tier_moves).map_err(|_| {
        TsinkError::DataCorruption(
            "post-flush replacement tier-move count exceeds the supported range".to_string(),
        )
    })?;
    if usize::try_from(marker.tier_moves)
        .ok()
        .is_none_or(|tier_moves| tier_moves > marker.outputs.len())
    {
        return Err(TsinkError::DataCorruption(
            "post-flush replacement tier-move count exceeds its output count".to_string(),
        ));
    }

    let mut source_roots = BTreeSet::new();
    let mut sources = Vec::with_capacity(marker.sources.len());
    for source in &marker.sources {
        let segment = resolve_segment_record(resolver, &source.segment)?;
        if !source_roots.insert(segment.root.clone()) {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement repeats source segment {}",
                segment.root.display()
            )));
        }
        sources.push(ValidatedSourceRecord {
            segment,
            counts_as_expired: source.counts_as_expired,
        });
    }
    let mut output_roots = BTreeSet::new();
    let mut outputs = Vec::with_capacity(marker.outputs.len());
    for output in &marker.outputs {
        let segment = resolve_segment_record(resolver, output)?;
        if source_roots.contains(&segment.root) {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement uses one path as both source and output: {}",
                segment.root.display()
            )));
        }
        if !output_roots.insert(segment.root.clone()) {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement repeats output segment {}",
                segment.root.display()
            )));
        }
        outputs.push(segment);
    }

    Ok(PostFlushReplacement {
        marker_path: marker_path.to_path_buf(),
        marker,
        sources,
        outputs,
    })
}

fn regular_file_read_options() -> fs::OpenOptions {
    let mut options = fs::OpenOptions::new();
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
    options
}

fn bounded_marker_cursor_reservation_bytes(data_path: &Path) -> usize {
    BOUNDED_POST_FLUSH_MARKER_CURSOR_BASE_BYTES.saturating_add(
        replacement_marker_dir(data_path)
            .as_os_str()
            .as_encoded_bytes()
            .len(),
    )
}

fn bounded_marker_parse_reservation_bytes(marker_path: &Path, marker_bytes: u64) -> usize {
    usize::try_from(marker_bytes)
        .unwrap_or(usize::MAX)
        .saturating_mul(BOUNDED_POST_FLUSH_MARKER_PARSE_PAYLOAD_COPIES)
        .saturating_add(BOUNDED_POST_FLUSH_MARKER_PARSE_BASE_BYTES)
        .saturating_add(
            marker_path
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .saturating_mul(2),
        )
}

impl BackgroundPostFlushRecoveryCursor {
    pub(super) fn reset(&mut self) {
        self.scan = None;
    }

    fn begin_scan(
        &mut self,
        data_path: &Path,
        memory_reservation: RemoteCatalogMemoryReservation,
    ) -> Result<bool> {
        debug_assert!(self.scan.is_none());
        let marker_dir = replacement_marker_dir(data_path);
        let metadata = match fs::symlink_metadata(&marker_dir) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: marker_dir,
                    source,
                })
            }
        };
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement marker root is link-like or not a directory: {}",
                marker_dir.display()
            )));
        }
        // Establish the exact namespace state as durable before a finite cursor starts
        // interpreting marker phases. A later marker entry is synchronized again before parse.
        crate::engine::fs_utils::sync_dir(&marker_dir)?;
        let entries = fs::read_dir(&marker_dir).map_err(|source| TsinkError::IoWithPath {
            path: marker_dir.clone(),
            source,
        })?;
        self.scan = Some(BackgroundPostFlushMarkerScan {
            marker_dir,
            entries,
            observed_entries: 0,
            _memory_reservation: memory_reservation,
        });
        Ok(true)
    }

    fn next_marker_path(&mut self) -> Result<BoundedMarkerPathStep> {
        let scan = self
            .scan
            .as_mut()
            .expect("bounded post-flush marker scan must be initialized");
        let Some(entry) = scan.entries.next() else {
            self.scan = None;
            return Ok(BoundedMarkerPathStep::NoPending);
        };
        if scan.observed_entries == crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: BOUNDED_POST_FLUSH_RECOVERY_OPERATION,
                limit: crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
                required: scan.observed_entries.saturating_add(1),
            });
        }
        scan.observed_entries = scan.observed_entries.checked_add(1).ok_or_else(|| {
            TsinkError::Other(format!(
                "post-flush replacement marker namespace entry counter overflow at {}",
                scan.marker_dir.display()
            ))
        })?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: scan.marker_dir.clone(),
            source,
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Ok(BoundedMarkerPathStep::EntryConsumed);
        };
        if !is_post_flush_replacement_marker_name(&name) {
            return Ok(BoundedMarkerPathStep::EntryConsumed);
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement marker entry is link-like or not regular: {}",
                path.display()
            )));
        }
        Ok(BoundedMarkerPathStep::Marker {
            path,
            marker_bytes: metadata.len(),
        })
    }
}

enum BoundedMarkerPathStep {
    NoPending,
    EntryConsumed,
    Marker { path: PathBuf, marker_bytes: u64 },
}

fn validate_finite_recovery_envelope(
    selected_items: usize,
    selected_bytes: u64,
    item_limit: usize,
    byte_limit: u64,
) -> Result<()> {
    if selected_items > item_limit {
        return Err(TsinkError::MaintenanceDependencyWindowExceeded {
            operation: BOUNDED_POST_FLUSH_RECOVERY_OPERATION,
            item_limit,
            byte_limit,
            selected_items,
            selected_bytes,
        });
    }
    if selected_bytes > byte_limit {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: BOUNDED_POST_FLUSH_RECOVERY_OPERATION,
            limit: byte_limit,
            required: selected_bytes,
        });
    }
    Ok(())
}

fn next_marker_path(data_path: &Path) -> Result<Option<PathBuf>> {
    let marker_dir = replacement_marker_dir(data_path);
    let metadata = match fs::symlink_metadata(&marker_dir) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: marker_dir,
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker root is link-like or not a directory: {}",
            marker_dir.display()
        )));
    }
    let mut markers = Vec::new();
    for entry in crate::engine::fs_utils::collect_directory_entries_bounded(
        &marker_dir,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        "post-flush replacement marker recovery",
    )? {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_post_flush_replacement_marker_name(&name) {
            continue;
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
                "post-flush replacement marker entry is link-like or not regular: {}",
                entry.path().display()
            )));
        }
        markers.push(entry.path());
    }
    markers.sort();
    Ok(markers.into_iter().next())
}

fn load_next_marker(
    data_path: &Path,
    resolver: SegmentPathResolver<'_>,
) -> Result<Option<PostFlushReplacement>> {
    let Some(marker_path) = next_marker_path(data_path)? else {
        return Ok(None);
    };
    // A prior marker write may have returned after its rename but before parent synchronization.
    // Establish the visible state as durable before interpreting its phase.
    crate::engine::fs_utils::sync_parent_dir(&marker_path)?;
    parse_marker(data_path, resolver, &marker_path).map(Some)
}

pub(super) fn publish_prepared_replacement(
    data_path: &Path,
    resolver: SegmentPathResolver<'_>,
    sources: &[RetiredPostFlushRoot],
    promotions: &[StagedSegmentPromotion],
    tier_moves: usize,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<PostFlushReplacement> {
    if sources.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "post-flush replacement requires at least one source".to_string(),
        ));
    }

    let mut source_roots = BTreeSet::new();
    let mut validated_sources = Vec::with_capacity(sources.len());
    let mut source_records = Vec::with_capacity(sources.len());
    for source in sources {
        let segment = locate_segment_record(resolver, &source.root)?;
        validate_complete_segment(&segment, &segment.root)?;
        if !source_roots.insert(segment.root.clone()) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "post-flush replacement repeats source segment {}",
                segment.root.display()
            )));
        }
        source_records.push(PostFlushSourceRecord {
            segment: segment.record.clone(),
            counts_as_expired: source.counts_as_expired,
        });
        validated_sources.push(ValidatedSourceRecord {
            segment,
            counts_as_expired: source.counts_as_expired,
        });
    }

    let mut output_roots = BTreeSet::new();
    let mut validated_outputs = Vec::with_capacity(promotions.len());
    let mut output_records = Vec::with_capacity(promotions.len());
    for promotion in promotions {
        let segment = locate_segment_record(resolver, &promotion.final_root)?;
        validate_complete_segment(&segment, &promotion.staging_root)?;
        require_absent(&segment.root, "final output")?;
        if source_roots.contains(&segment.root) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "post-flush replacement uses one path as both source and output: {}",
                segment.root.display()
            )));
        }
        if !output_roots.insert(segment.root.clone()) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "post-flush replacement repeats output segment {}",
                segment.root.display()
            )));
        }
        output_records.push(segment.record.clone());
        validated_outputs.push(segment);
    }

    let (id, marker_path) = allocate_marker(data_path)?;
    let marker = PostFlushReplacementMarker {
        version: POST_FLUSH_REPLACEMENT_VERSION,
        phase: PostFlushReplacementPhase::Prepared,
        id,
        sources: source_records,
        outputs: output_records,
        tier_moves: u64::try_from(tier_moves).map_err(|_| {
            TsinkError::Other(
                "post-flush replacement tier-move count exceeds the supported range".to_string(),
            )
        })?,
    };
    let replacement = PostFlushReplacement {
        marker_path,
        marker,
        sources: validated_sources,
        outputs: validated_outputs,
    };

    for index in 0..replacement.sources.len() {
        require_absent(
            &source_retirement_path(&replacement, index)?,
            "retirement target",
        )?;
    }
    for index in 0..replacement.outputs.len() {
        require_absent(
            &output_rollback_path(&replacement, index)?,
            "rollback target",
        )?;
    }

    require_absent(&replacement.marker_path, "marker target")?;
    write_marker_and_resolve_ambiguity(
        data_path,
        resolver,
        &replacement.marker_path,
        &replacement.marker,
        local_disk_budget,
    )?;
    Ok(replacement)
}

impl PostFlushReplacement {
    fn finite_recovery_item_count(&self) -> Result<usize> {
        self.sources
            .len()
            .checked_add(self.outputs.len())
            .ok_or_else(|| {
                TsinkError::Other(
                    "post-flush replacement finite recovery item count overflowed".to_string(),
                )
            })
    }

    fn preflight_bounded_recovery(&self) -> Result<(Vec<OwnedSegmentState>, u64, usize)> {
        let mut roots = Vec::with_capacity(self.sources.len().saturating_add(self.outputs.len()));
        let states = match self.marker.phase {
            PostFlushReplacementPhase::Prepared => {
                for (index, source) in self.sources.iter().enumerate() {
                    require_absent(
                        &source_retirement_path(self, index)?,
                        "retirement target while marker is Prepared",
                    )?;
                    roots.push(source.segment.root.clone());
                }
                let states = (0..self.outputs.len())
                    .map(|index| self.output_state_without_validation(index))
                    .collect::<Result<Vec<_>>>()?;
                for (index, state) in states.iter().copied().enumerate() {
                    match state {
                        OwnedSegmentState::Live => roots.push(self.outputs[index].root.clone()),
                        OwnedSegmentState::Retired => {
                            roots.push(output_rollback_path(self, index)?);
                        }
                        OwnedSegmentState::Gone => {}
                    }
                }
                states
            }
            PostFlushReplacementPhase::Committing => {
                for (index, output) in self.outputs.iter().enumerate() {
                    require_absent(
                        &output_rollback_path(self, index)?,
                        "rollback target while marker is Committing",
                    )?;
                    roots.push(output.root.clone());
                }
                let states = (0..self.sources.len())
                    .map(|index| self.source_state_without_validation(index))
                    .collect::<Result<Vec<_>>>()?;
                if states.contains(&OwnedSegmentState::Live)
                    && states.contains(&OwnedSegmentState::Gone)
                {
                    return Err(TsinkError::DataCorruption(
                        "post-flush replacement has an unowned missing source while another source remains visible"
                            .to_string(),
                    ));
                }
                for (index, state) in states.iter().copied().enumerate() {
                    match state {
                        OwnedSegmentState::Live => {
                            roots.push(self.sources[index].segment.root.clone());
                        }
                        OwnedSegmentState::Retired => {
                            roots.push(source_retirement_path(self, index)?);
                        }
                        OwnedSegmentState::Gone => {}
                    }
                }
                states
            }
        };

        let mut source_bytes = 0u64;
        // This reservation remains live through catalog-transition construction. In addition to
        // each segment decoder's conservative runtime preflight, include the marker-root clones,
        // output/source descriptor vectors, and registry-delta/path handoffs that are built before
        // the exact transition object exists and can be modeled a second time.
        let mut reservation_bytes = roots
            .len()
            .saturating_mul(BOUNDED_POST_FLUSH_TRANSITION_RECORD_BYTES);
        for root in roots {
            let metadata =
                fs::symlink_metadata(&root).map_err(|source| TsinkError::IoWithPath {
                    path: root.clone(),
                    source,
                })?;
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_dir()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "post-flush replacement recovery root is link-like or not a directory: {}",
                    root.display()
                )));
            }
            let preflight =
                super::super::super::tiering::preflight_segment_runtime_refresh_memory(&root)?;
            source_bytes = source_bytes
                .checked_add(preflight.source_bytes)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush replacement source byte preflight overflowed".to_string(),
                    )
                })?;
            reservation_bytes = reservation_bytes
                .saturating_add(preflight.reservation_bytes)
                .saturating_add(
                    root.as_os_str()
                        .as_encoded_bytes()
                        .len()
                        .saturating_mul(BOUNDED_POST_FLUSH_TRANSITION_PATH_COPIES),
                );
        }
        Ok((states, source_bytes, reservation_bytes))
    }

    fn prepare_bounded_prepared_recovery(
        &self,
        expected_states: &[OwnedSegmentState],
    ) -> Result<Vec<OwnedSegmentState>> {
        self.validate_prepared_sources()?;
        let states = (0..self.outputs.len())
            .map(|index| self.output_state(index))
            .collect::<Result<Vec<_>>>()?;
        if states != expected_states {
            return Err(TsinkError::DataCorruption(
                "Prepared post-flush replacement paths changed after recovery preflight"
                    .to_string(),
            ));
        }
        Ok(states)
    }

    fn prepare_bounded_committing_recovery(
        &self,
        expected_states: &[OwnedSegmentState],
    ) -> Result<(Vec<OwnedSegmentState>, Vec<SegmentInventoryEntry>)> {
        let states = self.validate_committing_state()?;
        if states != expected_states {
            return Err(TsinkError::DataCorruption(
                "Committing post-flush replacement paths changed after recovery preflight"
                    .to_string(),
            ));
        }
        let mut output_entries = Vec::with_capacity(self.outputs.len());
        for output in &self.outputs {
            let segment = validate_complete_segment(output, &output.root)?;
            output_entries.push(SegmentInventoryEntry {
                lane: output.record.lane,
                tier: output.record.tier,
                root: output.root.clone(),
                manifest: segment.manifest,
            });
        }
        Ok((states, output_entries))
    }

    pub(super) fn mark_committing(
        &mut self,
        data_path: &Path,
        resolver: SegmentPathResolver<'_>,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        if self.marker.phase != PostFlushReplacementPhase::Prepared {
            return Err(TsinkError::Other(
                "post-flush replacement is already committing".to_string(),
            ));
        }
        self.validate_prepared_sources()?;
        for (index, output) in self.outputs.iter().enumerate() {
            validate_complete_segment(output, &output.root)?;
            require_absent(&output_rollback_path(self, index)?, "rollback target")?;
        }

        let mut committing = self.marker.clone();
        committing.phase = PostFlushReplacementPhase::Committing;
        write_marker_and_resolve_ambiguity(
            data_path,
            resolver,
            &self.marker_path,
            &committing,
            local_disk_budget,
        )?;
        self.marker = committing;
        Ok(())
    }

    fn validate_prepared_sources(&self) -> Result<()> {
        for (index, source) in self.sources.iter().enumerate() {
            validate_complete_segment(&source.segment, &source.segment.root)?;
            require_absent(
                &source_retirement_path(self, index)?,
                "retirement target while marker is Prepared",
            )?;
        }
        Ok(())
    }

    fn output_state(&self, index: usize) -> Result<OwnedSegmentState> {
        let output = &self.outputs[index];
        let rollback = output_rollback_path(self, index)?;
        let state = self.output_state_without_validation(index)?;
        match state {
            OwnedSegmentState::Live => {
                validate_complete_segment(output, &output.root)?;
            }
            OwnedSegmentState::Retired => {
                validate_complete_segment(output, &rollback)?;
            }
            OwnedSegmentState::Gone => {}
        }
        Ok(state)
    }

    fn output_state_without_validation(&self, index: usize) -> Result<OwnedSegmentState> {
        let output = &self.outputs[index];
        let rollback = output_rollback_path(self, index)?;
        match (entry_metadata(&output.root)?, entry_metadata(&rollback)?) {
            (Some(_), None) => Ok(OwnedSegmentState::Live),
            (None, Some(_)) => Ok(OwnedSegmentState::Retired),
            (None, None) => Ok(OwnedSegmentState::Gone),
            (Some(_), Some(_)) => Err(TsinkError::DataCorruption(format!(
                "post-flush Prepared output and rollback target both exist: output={}, rollback={}",
                output.root.display(),
                rollback.display()
            ))),
        }
    }

    pub(super) fn rollback_prepared(
        &self,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        if self.marker.phase != PostFlushReplacementPhase::Prepared {
            return Err(TsinkError::Other(
                "refusing to roll back a committing post-flush replacement".to_string(),
            ));
        }
        // Validate every source and every transaction-owned output before the first rename. The
        // marker intentionally does not persist raw staging paths: after every known final is
        // rolled back and the marker is removed, startup's exact-shape orphan cleanup owns those
        // leftover stage roots.
        self.validate_prepared_sources()?;
        let states = (0..self.outputs.len())
            .map(|index| self.output_state(index))
            .collect::<Result<Vec<_>>>()?;
        self.rollback_prepared_from_states(local_disk_budget, &states)
    }

    fn rollback_prepared_from_states(
        &self,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        states: &[OwnedSegmentState],
    ) -> Result<()> {
        let operation = || self.rollback_prepared_with_states(states);
        let Some(budget) = local_disk_budget else {
            return operation();
        };
        let peak = recovery_rename_peak_bytes(
            budget,
            states
                .iter()
                .enumerate()
                .filter(|(_, state)| **state == OwnedSegmentState::Live)
                .map(|(index, _)| {
                    output_rollback_path(self, index)
                        .map(|destination| (self.outputs[index].root.clone(), destination))
                }),
        )?;
        budget.with_reconciled_recovery_reservation(crate::DiskCategory::Temporary, peak, operation)
    }

    fn rollback_prepared_with_states(&self, states: &[OwnedSegmentState]) -> Result<()> {
        for (index, state) in states.iter().enumerate() {
            if *state != OwnedSegmentState::Live {
                continue;
            }
            let rollback = output_rollback_path(self, index)?;
            crate::engine::fs_utils::rename_and_sync_parents(&self.outputs[index].root, &rollback)?;
        }
        for index in 0..self.outputs.len() {
            let output = &self.outputs[index];
            let rollback = output_rollback_path(self, index)?;
            if entry_metadata(&output.root)?.is_some() {
                return Err(TsinkError::Other(format!(
                    "post-flush Prepared output remained visible after rollback: {}",
                    output.root.display()
                )));
            }
            if entry_metadata(&rollback)?.is_some() {
                validate_complete_segment(output, &rollback)?;
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&rollback)?;
            }
        }
        remove_marker_after_commit(&self.marker_path, None)
    }

    fn source_state(&self, index: usize) -> Result<OwnedSegmentState> {
        let source = &self.sources[index].segment;
        let retired = source_retirement_path(self, index)?;
        let state = self.source_state_without_validation(index)?;
        match state {
            OwnedSegmentState::Live => {
                validate_complete_segment(source, &source.root)?;
            }
            OwnedSegmentState::Retired => {
                validate_complete_segment(source, &retired)?;
            }
            OwnedSegmentState::Gone => {}
        }
        Ok(state)
    }

    fn source_state_without_validation(&self, index: usize) -> Result<OwnedSegmentState> {
        let source = &self.sources[index].segment;
        let retired = source_retirement_path(self, index)?;
        match (entry_metadata(&source.root)?, entry_metadata(&retired)?) {
            (Some(_), None) => Ok(OwnedSegmentState::Live),
            (None, Some(_)) => Ok(OwnedSegmentState::Retired),
            (None, None) => Ok(OwnedSegmentState::Gone),
            (Some(_), Some(_)) => Err(TsinkError::DataCorruption(format!(
                "post-flush source and retirement target both exist: source={}, retired={}",
                source.root.display(),
                retired.display()
            ))),
        }
    }

    fn validate_committing_state(&self) -> Result<Vec<OwnedSegmentState>> {
        if self.marker.phase != PostFlushReplacementPhase::Committing {
            return Err(TsinkError::Other(
                "post-flush replacement has not reached the commit point".to_string(),
            ));
        }
        for (index, output) in self.outputs.iter().enumerate() {
            validate_complete_segment(output, &output.root)?;
            require_absent(
                &output_rollback_path(self, index)?,
                "rollback target while marker is Committing",
            )?;
        }
        let states = (0..self.sources.len())
            .map(|index| self.source_state(index))
            .collect::<Result<Vec<_>>>()?;
        if states.contains(&OwnedSegmentState::Live) && states.contains(&OwnedSegmentState::Gone) {
            return Err(TsinkError::DataCorruption(
                "post-flush replacement has an unowned missing source while another source remains visible"
                    .to_string(),
            ));
        }
        Ok(states)
    }

    pub(super) fn output_entries(&self) -> Result<Vec<SegmentInventoryEntry>> {
        self.validate_committing_state()?;
        self.outputs
            .iter()
            .map(|output| {
                let segment = validate_complete_segment(output, &output.root)?;
                Ok(SegmentInventoryEntry {
                    lane: output.record.lane,
                    tier: output.record.tier,
                    root: output.root.clone(),
                    manifest: segment.manifest,
                })
            })
            .collect()
    }

    pub(super) fn source_roots(&self) -> Vec<PathBuf> {
        self.sources
            .iter()
            .map(|source| source.segment.root.clone())
            .collect()
    }

    pub(super) fn tier_moves(&self) -> usize {
        usize::try_from(self.marker.tier_moves)
            .expect("validated post-flush marker tier-move count must fit usize")
    }

    pub(super) fn finish_committing(
        &self,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<usize> {
        let states = self.validate_committing_state()?;
        self.finish_committing_from_states(local_disk_budget, &states)
    }

    fn finish_committing_from_states(
        &self,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        states: &[OwnedSegmentState],
    ) -> Result<usize> {
        let operation = || self.finish_committing_with_states(states);
        let Some(budget) = local_disk_budget else {
            return operation();
        };
        let peak = recovery_rename_peak_bytes(
            budget,
            states
                .iter()
                .enumerate()
                .filter(|(_, state)| **state == OwnedSegmentState::Live)
                .map(|(index, _)| {
                    source_retirement_path(self, index)
                        .map(|destination| (self.sources[index].segment.root.clone(), destination))
                }),
        )?;
        budget.with_reconciled_recovery_reservation(crate::DiskCategory::Temporary, peak, operation)
    }

    fn finish_committing_with_states(&self, states: &[OwnedSegmentState]) -> Result<usize> {
        for (index, state) in states.iter().copied().enumerate() {
            if state != OwnedSegmentState::Live {
                continue;
            }
            let retired = source_retirement_path(self, index)?;
            crate::engine::fs_utils::rename_and_sync_parents(
                &self.sources[index].segment.root,
                &retired,
            )?;
        }

        // No recursive deletion begins until every loader-visible source name has disappeared.
        for (index, state) in states.iter().copied().enumerate() {
            if state == OwnedSegmentState::Gone {
                continue;
            }
            let source = &self.sources[index].segment;
            let retired = source_retirement_path(self, index)?;
            if entry_metadata(&source.root)?.is_some() {
                return Err(TsinkError::Other(format!(
                    "post-flush source remained visible after retirement: {}",
                    source.root.display()
                )));
            }
            validate_complete_segment(source, &retired)?;
        }
        for (index, state) in states.iter().enumerate() {
            if *state == OwnedSegmentState::Gone {
                continue;
            }
            crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(
                &source_retirement_path(self, index)?,
            )?;
        }

        let expired = self
            .sources
            .iter()
            .filter(|source| source.counts_as_expired)
            .count();
        remove_marker_after_commit(&self.marker_path, None)?;
        Ok(expired)
    }
}

fn recovery_rename_peak_bytes<I>(budget: &Arc<crate::LocalDiskBudget>, paths: I) -> Result<u64>
where
    I: IntoIterator<Item = Result<(PathBuf, PathBuf)>>,
{
    let mut governed_renames = 0u64;
    for paths in paths {
        let (source, destination) = paths?;
        let source_governed = budget.governs_entry(&source)?;
        let destination_governed = budget.governs_entry(&destination)?;
        if source_governed != destination_governed {
            return Err(TsinkError::InvalidConfiguration(format!(
                "post-flush recovery rename crosses the managed disk boundary: source={}, destination={}",
                source.display(), destination.display()
            )));
        }
        if source_governed {
            governed_renames = governed_renames.checked_add(1).ok_or_else(|| {
                TsinkError::Other(
                    "post-flush recovery rename count exceeds the supported range".to_string(),
                )
            })?;
        }
    }
    governed_renames
        .checked_mul(budget.snapshot_restore_entry_staging_allowance_bytes()?)
        .ok_or_else(|| {
            TsinkError::Other(
                "post-flush recovery rename allowance exceeds the supported range".to_string(),
            )
        })
}

fn remove_marker_after_commit(
    marker_path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let result = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
        marker_path,
        local_disk_budget,
        crate::DiskCategory::Temporary,
    );
    let Err(removal_error) = result else {
        return Ok(());
    };
    match entry_metadata(marker_path) {
        Ok(Some(_)) => Err(removal_error),
        Ok(None) => {
            if let Err(sync_error) = crate::engine::fs_utils::sync_parent_dir(marker_path) {
                return Err(TsinkError::Other(format!(
                    "post-flush replacement committed but marker absence could not be proven durable: removal={removal_error}; retry_sync={sync_error}"
                )));
            }
            Ok(())
        }
        Err(probe_error) => {
            Err(TsinkError::Other(format!(
                "post-flush replacement committed but marker removal could not be proven: removal={removal_error}; probe={probe_error}"
            )))
        }
    }
}

/// Synchronizes the post-flush marker namespace and refuses any operation that could scan,
/// snapshot, compact, or otherwise mutate segment roots while a durable transaction remains.
///
/// Synchronizing before the scan is essential after an unlink whose parent sync previously
/// failed: an apparently absent marker is not a safe fence boundary until this exact directory is
/// durable.
pub(in crate::engine::storage_engine) fn ensure_no_pending_post_flush_replacement(
    data_path: &Path,
) -> Result<()> {
    let marker_dir = replacement_marker_dir(data_path);
    match fs::symlink_metadata(&marker_dir) {
        Ok(metadata)
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_dir() =>
        {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement marker root is link-like or not a directory: {}",
                marker_dir.display()
            )));
        }
        Ok(_) => crate::engine::fs_utils::sync_dir(&marker_dir)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: marker_dir,
                source,
            })
        }
    }
    if let Some(marker_path) = next_marker_path(data_path)? {
        return Err(TsinkError::Other(format!(
            "segment operation deferred while a durable post-flush replacement is pending: {}",
            marker_path.display()
        )));
    }
    Ok(())
}

pub(super) fn next_runtime_replacement(
    data_path: &Path,
    resolver: SegmentPathResolver<'_>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<Option<PostFlushReplacement>> {
    loop {
        let Some(replacement) = load_next_marker(data_path, resolver)? else {
            return Ok(None);
        };
        match replacement.marker.phase {
            PostFlushReplacementPhase::Prepared => {
                replacement.rollback_prepared(local_disk_budget)?;
            }
            PostFlushReplacementPhase::Committing => {
                replacement.validate_committing_state()?;
                return Ok(Some(replacement));
            }
        }
    }
}

impl BoundedRuntimeReplacement {
    pub(super) fn replacement(&self) -> &PostFlushReplacement {
        &self.replacement
    }

    pub(super) fn output_entries(&self) -> &[SegmentInventoryEntry] {
        &self.output_entries
    }

    pub(super) fn source_roots(&self) -> &[PathBuf] {
        &self.source_roots
    }

    pub(super) fn selected_items(&self) -> usize {
        self.selected_items
    }

    pub(super) fn selected_bytes(&self) -> u64 {
        self.selected_bytes
    }

    pub(super) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_bytes
    }

    pub(super) fn finish_committing(
        &self,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<usize> {
        self.replacement
            .finish_committing_from_states(local_disk_budget, &self.source_states)
    }
}

pub(super) fn next_runtime_replacement_bounded<F>(
    cursor: &mut BackgroundPostFlushRecoveryCursor,
    data_path: &Path,
    resolver: SegmentPathResolver<'_>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    item_limit: usize,
    byte_limit: u64,
    mut reserve_memory: F,
) -> Result<BoundedRuntimeReplacementStep>
where
    F: FnMut(usize) -> Result<RemoteCatalogMemoryReservation>,
{
    let outcome = (|| {
        if cursor.scan.is_none() {
            let reservation = reserve_memory(bounded_marker_cursor_reservation_bytes(data_path))?;
            if !cursor.begin_scan(data_path, reservation)? {
                return Ok(BoundedRuntimeReplacementStep::NoPending);
            }
        }
        let marker = match cursor.next_marker_path()? {
            BoundedMarkerPathStep::NoPending => {
                return Ok(BoundedRuntimeReplacementStep::NoPending)
            }
            BoundedMarkerPathStep::EntryConsumed => {
                return Ok(BoundedRuntimeReplacementStep::NamespaceEntryConsumed)
            }
            BoundedMarkerPathStep::Marker { path, marker_bytes } => (path, marker_bytes),
        };
        let (marker_path, marker_bytes) = marker;
        if marker_bytes > MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement marker exceeds the {} byte limit: {}",
                MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES,
                marker_path.display()
            )));
        }
        let marker_memory_bytes =
            bounded_marker_parse_reservation_bytes(&marker_path, marker_bytes);
        let marker_memory_reservation = reserve_memory(marker_memory_bytes)?;
        // A prior marker write may have returned after its rename but before parent
        // synchronization. Establish this exact visible marker as durable before interpreting
        // its phase.
        crate::engine::fs_utils::sync_parent_dir(&marker_path)?;
        let replacement = parse_marker(data_path, resolver, &marker_path)?;
        let selected_items = replacement.finite_recovery_item_count()?;
        if selected_items > item_limit {
            validate_finite_recovery_envelope(
                selected_items,
                marker_bytes.max(u64::try_from(marker_memory_bytes).unwrap_or(u64::MAX)),
                item_limit,
                byte_limit,
            )?;
            unreachable!("finite recovery item validation must reject an oversized marker");
        }
        // The marker contains only paths. Inspect fixed file lengths/headers for every source and
        // output first, then admit their aggregate decode/index capacity before the first full
        // segment validation or runtime load.
        let (preflight_states, source_bytes, segment_memory_bytes) =
            replacement.preflight_bounded_recovery()?;
        let segment_memory_reservation = reserve_memory(segment_memory_bytes)?;
        let logical_bytes = marker_bytes.checked_add(source_bytes).ok_or_else(|| {
            TsinkError::Other(
                "post-flush replacement finite recovery byte count overflowed".to_string(),
            )
        })?;
        let retained_memory_bytes = bounded_marker_cursor_reservation_bytes(data_path)
            .saturating_add(marker_memory_bytes)
            .saturating_add(segment_memory_bytes);
        let selected_bytes =
            logical_bytes.max(u64::try_from(retained_memory_bytes).unwrap_or(u64::MAX));
        validate_finite_recovery_envelope(selected_items, selected_bytes, item_limit, byte_limit)?;

        match replacement.marker.phase {
            PostFlushReplacementPhase::Prepared => {
                let states = replacement.prepare_bounded_prepared_recovery(&preflight_states)?;
                replacement.rollback_prepared_from_states(local_disk_budget, &states)?;
                drop(segment_memory_reservation);
                drop(marker_memory_reservation);
                Ok(BoundedRuntimeReplacementStep::PreparedRolledBack)
            }
            PostFlushReplacementPhase::Committing => {
                let (source_states, output_entries) =
                    replacement.prepare_bounded_committing_recovery(&preflight_states)?;
                let source_roots = replacement.source_roots();
                Ok(BoundedRuntimeReplacementStep::Committing(Box::new(
                    BoundedRuntimeReplacement {
                        replacement,
                        source_states,
                        output_entries,
                        source_roots,
                        selected_items,
                        selected_bytes,
                        retained_memory_bytes,
                        _marker_memory_reservation: marker_memory_reservation,
                        _segment_memory_reservation: segment_memory_reservation,
                    },
                )))
            }
        }
    })();
    if outcome.is_err() {
        cursor.reset();
    }
    outcome
}

#[derive(Debug)]
struct StartupMarkerFile {
    path: PathBuf,
    length: usize,
}

#[derive(Debug)]
struct StartupPendingReplacement {
    replacement: PostFlushReplacement,
    states: Vec<OwnedSegmentState>,
}

fn modeled_startup_marker_files_bytes(files: &Vec<StartupMarkerFile>) -> Result<usize> {
    files.iter().try_fold(
        std::mem::size_of::<Vec<StartupMarkerFile>>()
            .checked_add(
                files
                    .capacity()
                    .checked_mul(std::mem::size_of::<StartupMarkerFile>())
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "post-flush startup marker vector capacity overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                TsinkError::Other("post-flush startup marker memory overflow".to_string())
            })?,
        |total, marker| {
            total.checked_add(marker.path.capacity()).ok_or_else(|| {
                TsinkError::Other("post-flush startup marker path overflow".to_string())
            })
        },
    )
}

fn push_startup_marker_file(
    files: &mut Vec<StartupMarkerFile>,
    marker: StartupMarkerFile,
    memory_budget_bytes: usize,
) -> Result<()> {
    let prospective_capacity = if files.len() == files.capacity() {
        files.len().checked_add(1).ok_or_else(|| {
            TsinkError::Other("post-flush startup marker count overflow".to_string())
        })?
    } else {
        files.capacity()
    };
    let path_bytes = files
        .iter()
        .try_fold(marker.path.capacity(), |total, file| {
            total.checked_add(file.path.capacity()).ok_or_else(|| {
                TsinkError::Other("post-flush startup marker path overflow".to_string())
            })
        })?;
    let required = std::mem::size_of::<Vec<StartupMarkerFile>>()
        .checked_add(
            prospective_capacity
                .checked_mul(std::mem::size_of::<StartupMarkerFile>())
                .ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush startup marker vector capacity overflow".to_string(),
                    )
                })?,
        )
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .ok_or_else(|| {
            TsinkError::Other("post-flush startup marker memory overflow".to_string())
        })?;
    crate::disk_budget::admit_startup_memory(memory_budget_bytes, required)?;
    if files.len() == files.capacity() {
        files.try_reserve_exact(1).map_err(|_| {
            TsinkError::Other(
                "unable to allocate bounded post-flush startup marker list".to_string(),
            )
        })?;
    }
    files.push(marker);
    crate::disk_budget::admit_startup_memory(
        memory_budget_bytes,
        modeled_startup_marker_files_bytes(files)?,
    )
}

fn enumerate_startup_marker_files(
    data_path: &Path,
    memory_budget_bytes: usize,
) -> Result<Vec<StartupMarkerFile>> {
    let marker_dir = replacement_marker_dir(data_path);
    let metadata = match fs::symlink_metadata(&marker_dir) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: marker_dir,
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush replacement marker root is link-like or not a directory: {}",
            marker_dir.display()
        )));
    }
    let mut files = Vec::new();
    crate::disk_budget::admit_startup_memory(
        memory_budget_bytes,
        modeled_startup_marker_files_bytes(&files)?
            .checked_add(std::mem::size_of::<fs::ReadDir>())
            .ok_or_else(|| {
                TsinkError::Other("post-flush startup marker memory overflow".to_string())
            })?,
    )?;
    let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
    );
    let entries = fs::read_dir(&marker_dir).map_err(|source| TsinkError::IoWithPath {
        path: marker_dir.clone(),
        source,
    })?;
    for entry in entries {
        namespace_budget.observe_entry(
            &marker_dir,
            "post-flush replacement startup marker recovery",
        )?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: marker_dir.clone(),
            source,
        })?;
        let name = entry.file_name();
        let component_bytes = name.as_encoded_bytes().len();
        let anticipated_path_bytes = marker_dir
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(component_bytes)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| {
                TsinkError::Other("post-flush startup marker path model overflow".to_string())
            })?;
        let transient = modeled_startup_marker_files_bytes(&files)?
            .checked_add(std::mem::size_of::<fs::ReadDir>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<fs::DirEntry>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
            .and_then(|bytes| bytes.checked_add(component_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
            .and_then(|bytes| bytes.checked_add(anticipated_path_bytes))
            .ok_or_else(|| {
                TsinkError::Other("post-flush startup marker memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(memory_budget_bytes, transient)?;
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_post_flush_replacement_marker_name(name) {
            continue;
        }
        let path = marker_dir.join(entry.file_name());
        let metadata = fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement marker entry is link-like or not regular: {}",
                path.display()
            )));
        }
        if metadata.len() > MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush replacement marker exceeds the {} byte limit: {}",
                MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES,
                path.display()
            )));
        }
        let length = usize::try_from(metadata.len()).map_err(|_| {
            TsinkError::DataCorruption(format!(
                "post-flush replacement marker length exceeds this platform: {}",
                path.display()
            ))
        })?;
        push_startup_marker_file(
            &mut files,
            StartupMarkerFile { path, length },
            memory_budget_bytes,
        )?;
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn startup_marker_record_upper_bound(marker_bytes: usize) -> usize {
    // Even `{}` consumes two bytes. This deliberately loose bound remains proportional for small
    // markers while the format's explicit record limit caps adversarial large inputs.
    marker_bytes
        .checked_div(2)
        .unwrap_or(0)
        .saturating_add(1)
        .min(MAX_POST_FLUSH_REPLACEMENT_RECORDS)
}

fn startup_marker_retained_upper_bound(
    marker_bytes: usize,
    configured_root_path_bytes: usize,
) -> Result<usize> {
    let records = startup_marker_record_upper_bound(marker_bytes);
    let per_record = std::mem::size_of::<PostFlushSourceRecord>()
        .checked_add(std::mem::size_of::<PostFlushSegmentRecord>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ValidatedSourceRecord>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ValidatedSegmentRecord>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<OwnedSegmentState>()))
        // Validated root plus the temporary uniqueness-set clone and path/container slack.
        .and_then(|bytes| bytes.checked_add(configured_root_path_bytes.saturating_mul(2)))
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(|| {
            TsinkError::Other("post-flush startup record memory model overflow".to_string())
        })?;
    std::mem::size_of::<StartupPendingReplacement>()
        .checked_add(marker_bytes.saturating_mul(3))
        .and_then(|bytes| bytes.checked_add(records.saturating_mul(per_record)))
        .ok_or_else(|| {
            TsinkError::Other("post-flush startup marker memory model overflow".to_string())
        })
}

fn startup_recovery_memory_upper_bound(
    marker_files: &Vec<StartupMarkerFile>,
    configured_root_path_bytes: usize,
) -> Result<usize> {
    let marker_file_bytes = modeled_startup_marker_files_bytes(marker_files)?;
    let pending_vector_bytes = std::mem::size_of::<Vec<StartupPendingReplacement>>()
        .checked_add(
            marker_files
                .len()
                .checked_mul(std::mem::size_of::<StartupPendingReplacement>())
                .ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush startup pending vector capacity overflow".to_string(),
                    )
                })?,
        )
        .ok_or_else(|| {
            TsinkError::Other("post-flush startup pending memory overflow".to_string())
        })?;
    let mut retained = marker_file_bytes
        .checked_add(pending_vector_bytes)
        .ok_or_else(|| {
            TsinkError::Other("post-flush startup retained memory overflow".to_string())
        })?;
    let mut largest_read_buffer = 0usize;
    for marker in marker_files {
        retained = retained
            .checked_add(startup_marker_retained_upper_bound(
                marker.length,
                configured_root_path_bytes,
            )?)
            .ok_or_else(|| {
                TsinkError::Other("post-flush startup retained memory overflow".to_string())
            })?;
        largest_read_buffer = largest_read_buffer.max(marker.length.saturating_mul(2));
    }
    retained
        .checked_add(largest_read_buffer)
        .ok_or_else(|| TsinkError::Other("post-flush startup peak memory overflow".to_string()))
}

fn validate_startup_replacement_set(pending: &[StartupPendingReplacement]) -> Result<()> {
    for (index, current) in pending.iter().enumerate() {
        for prior in &pending[..index] {
            for current_root in current
                .replacement
                .sources
                .iter()
                .map(|source| &source.segment.root)
                .chain(
                    current
                        .replacement
                        .outputs
                        .iter()
                        .map(|output| &output.root),
                )
            {
                if prior
                    .replacement
                    .sources
                    .iter()
                    .map(|source| &source.segment.root)
                    .chain(prior.replacement.outputs.iter().map(|output| &output.root))
                    .any(|prior_root| prior_root == current_root)
                {
                    return Err(TsinkError::DataCorruption(format!(
                        "post-flush startup markers overlap on segment root {}",
                        current_root.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn load_startup_replacements(
    data_path: &Path,
    resolver: SegmentPathResolver<'_>,
    configured_root_path_bytes: usize,
    memory_budget_bytes: usize,
) -> Result<Vec<StartupPendingReplacement>> {
    let marker_files = enumerate_startup_marker_files(data_path, memory_budget_bytes)?;
    let required = startup_recovery_memory_upper_bound(&marker_files, configured_root_path_bytes)?;
    crate::disk_budget::admit_startup_memory(memory_budget_bytes, required)?;
    let mut pending = Vec::new();
    pending.try_reserve_exact(marker_files.len()).map_err(|_| {
        TsinkError::Other("unable to allocate bounded post-flush startup state".to_string())
    })?;
    for marker in &marker_files {
        // A prior marker write may have renamed successfully before its parent sync failed.
        crate::engine::fs_utils::sync_parent_dir(&marker.path)?;
        let replacement = parse_marker(data_path, resolver, &marker.path)?;
        let states = match replacement.marker.phase {
            PostFlushReplacementPhase::Prepared => {
                replacement.validate_prepared_sources()?;
                (0..replacement.outputs.len())
                    .map(|index| replacement.output_state(index))
                    .collect::<Result<Vec<_>>>()?
            }
            PostFlushReplacementPhase::Committing => replacement.validate_committing_state()?,
        };
        pending.push(StartupPendingReplacement {
            replacement,
            states,
        });
    }
    validate_startup_replacement_set(&pending)?;
    Ok(pending)
}

pub(in crate::engine::storage_engine) fn finalize_pending_post_flush_replacements_for_startup(
    data_path: &Path,
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&super::super::super::config::TieredStorageConfig>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget_bytes: usize,
) -> Result<()> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let configured_root_path_bytes = numeric_lane_path
        .into_iter()
        .chain(blob_lane_path)
        .map(|path| path.as_os_str().as_encoded_bytes().len())
        .chain(tiered_storage.into_iter().map(|config| {
            config
                .object_store_root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .saturating_add(128)
        }))
        .max()
        .unwrap_or(data_path.as_os_str().as_encoded_bytes().len())
        .saturating_add(64);
    let pending = load_startup_replacements(
        data_path,
        resolver,
        configured_root_path_bytes,
        memory_budget_bytes,
    )?;
    for pending in pending {
        match pending.replacement.marker.phase {
            PostFlushReplacementPhase::Prepared => {
                pending
                    .replacement
                    .rollback_prepared_from_states(local_disk_budget, &pending.states)?;
            }
            PostFlushReplacementPhase::Committing => {
                // Startup owns no live catalog yet. Completing exact filesystem retirement before
                // inventory discovery makes the first scan observe only the committed path set.
                pending
                    .replacement
                    .finish_committing_from_states(local_disk_budget, &pending.states)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;

    use parking_lot::Mutex;
    use tempfile::TempDir;

    use super::*;
    use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::{SegmentWriter, WalHighWatermark};
    use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
    use crate::{Label, LocalDiskLimits, Value};

    fn write_test_segment(lane_root: &Path, segment_id: u64) -> PathBuf {
        let registry = SeriesRegistry::new();
        let series = registry
            .resolve_or_insert(
                "post_flush_recovery",
                &[Label::new("segment", segment_id.to_string())],
            )
            .unwrap();
        let points = vec![
            ChunkPoint {
                ts: 10,
                value: Value::F64(segment_id as f64),
            },
            ChunkPoint {
                ts: 20,
                value: Value::F64(segment_id as f64 + 1.0),
            },
        ];
        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        let chunk = Chunk {
            header: ChunkHeader {
                series_id: series.series_id,
                lane: ValueLane::Numeric,
                value_family: Some(SeriesValueFamily::F64),
                point_count: points.len() as u16,
                min_ts: 10,
                max_ts: 20,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points,
            encoded_payload: encoded.payload,
            wal_lowwater: WalHighWatermark::default(),
            wal_highwater: WalHighWatermark::default(),
        };
        let chunks = HashMap::from([(series.series_id, vec![chunk])]);
        let writer = SegmentWriter::new(lane_root, 0, segment_id).unwrap();
        writer.write_segment(&registry, &chunks).unwrap();
        writer.layout().root.clone()
    }

    fn source(root: &Path, counts_as_expired: bool) -> RetiredPostFlushRoot {
        RetiredPostFlushRoot {
            root: root.to_path_buf(),
            counts_as_expired,
        }
    }

    fn promotion(staging_root: &Path, final_root: &Path) -> StagedSegmentPromotion {
        StagedSegmentPromotion {
            staging_root: staging_root.to_path_buf(),
            final_root: final_root.to_path_buf(),
        }
    }

    fn numeric_resolver(numeric_lane: &Path) -> SegmentPathResolver<'_> {
        SegmentPathResolver::new(Some(numeric_lane), None, None)
    }

    fn finalize_startup(data_path: &Path, numeric_lane: &Path) -> Result<()> {
        finalize_pending_post_flush_replacements_for_startup(
            data_path,
            Some(numeric_lane),
            None,
            None,
            None,
            usize::MAX,
        )
    }

    struct TestRecoveryMemory {
        accounting: Arc<super::super::super::RemoteCatalogMemoryAccounting>,
        write_transient: super::super::super::WriteTransientMemoryAccounting,
        admission_lock: Mutex<()>,
        used_bytes: AtomicU64,
        tombstone_staged_bytes: AtomicU64,
        budget_bytes: AtomicU64,
        rejections_total: AtomicU64,
    }

    impl TestRecoveryMemory {
        fn unlimited() -> Self {
            Self::with_budget(u64::MAX)
        }

        fn with_budget(budget_bytes: u64) -> Self {
            Self {
                accounting: Arc::new(super::super::super::RemoteCatalogMemoryAccounting::default()),
                write_transient: super::super::super::WriteTransientMemoryAccounting::default(),
                admission_lock: Mutex::new(()),
                used_bytes: AtomicU64::new(0),
                tombstone_staged_bytes: AtomicU64::new(0),
                budget_bytes: AtomicU64::new(budget_bytes),
                rejections_total: AtomicU64::new(0),
            }
        }

        fn reserve(&self, bytes: usize) -> Result<RemoteCatalogMemoryReservation> {
            self.accounting.new_reservation(
                bytes,
                super::super::super::MemoryReservationAdmissionContext {
                    reservation_admission_lock: &self.admission_lock,
                    used_bytes: &self.used_bytes,
                    tombstone_staged_bytes: &self.tombstone_staged_bytes,
                    remote_catalog_staging: &self.accounting,
                    write_transient: &self.write_transient,
                    budget_bytes: &self.budget_bytes,
                    memory_rejections_total: &self.rejections_total,
                },
            )
        }

        fn current_bytes(&self) -> usize {
            self.accounting.current_bytes()
        }
    }

    #[test]
    fn bounded_prepared_marker_recovery_pages_one_marker_per_wake_and_releases_memory() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let first_source = write_test_segment(&numeric_lane, 1);
        let second_source = write_test_segment(&numeric_lane, 2);
        let first = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&first_source, false)],
            &[],
            0,
            None,
        )
        .unwrap();
        let second = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&second_source, false)],
            &[],
            0,
            None,
        )
        .unwrap();
        let memory = TestRecoveryMemory::unlimited();
        let mut cursor = BackgroundPostFlushRecoveryCursor::default();

        let first_step = next_runtime_replacement_bounded(
            &mut cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            usize::MAX,
            u64::MAX,
            |bytes| memory.reserve(bytes),
        )
        .unwrap();
        assert!(matches!(
            first_step,
            BoundedRuntimeReplacementStep::PreparedRolledBack
        ));
        assert_eq!(
            usize::from(first.marker_path.exists()) + usize::from(second.marker_path.exists()),
            1,
            "one finite wake must roll back exactly one Prepared marker"
        );
        assert_eq!(
            memory.current_bytes(),
            bounded_marker_cursor_reservation_bytes(&data_path),
            "marker payload and aggregate segment decode reservations must release after rollback"
        );

        let second_step = next_runtime_replacement_bounded(
            &mut cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            usize::MAX,
            u64::MAX,
            |bytes| memory.reserve(bytes),
        )
        .unwrap();
        assert!(matches!(
            second_step,
            BoundedRuntimeReplacementStep::PreparedRolledBack
        ));
        assert!(!first.marker_path.exists());
        assert!(!second.marker_path.exists());

        let terminal = next_runtime_replacement_bounded(
            &mut cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            usize::MAX,
            u64::MAX,
            |bytes| memory.reserve(bytes),
        )
        .unwrap();
        assert!(matches!(terminal, BoundedRuntimeReplacementStep::NoPending));
        assert_eq!(memory.current_bytes(), 0);
    }

    #[test]
    fn bounded_committing_marker_requires_exact_multi_root_item_and_byte_envelope() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let first_source = write_test_segment(&numeric_lane, 1);
        let second_source = write_test_segment(&numeric_lane, 2);
        let staged_root = write_test_segment(&temp.path().join("staging"), 3);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000003");
        let mut replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&first_source, true), source(&second_source, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();
        replacement
            .mark_committing(&data_path, numeric_resolver(&numeric_lane), None)
            .unwrap();

        let marker_bytes = fs::symlink_metadata(&replacement.marker_path)
            .unwrap()
            .len();
        let marker_memory =
            bounded_marker_parse_reservation_bytes(&replacement.marker_path, marker_bytes);
        let (_, source_bytes, segment_memory) = replacement.preflight_bounded_recovery().unwrap();
        let required_items = replacement.finite_recovery_item_count().unwrap();
        let required_bytes = marker_bytes.checked_add(source_bytes).unwrap().max(
            u64::try_from(
                bounded_marker_cursor_reservation_bytes(&data_path)
                    .saturating_add(marker_memory)
                    .saturating_add(segment_memory),
            )
            .unwrap(),
        );
        assert_eq!(required_items, 3);

        let memory = TestRecoveryMemory::unlimited();
        let mut cursor = BackgroundPostFlushRecoveryCursor::default();
        let item_error = match next_runtime_replacement_bounded(
            &mut cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            required_items - 1,
            required_bytes,
            |bytes| memory.reserve(bytes),
        ) {
            Err(err) => err,
            Ok(_) => {
                panic!("N-1 roots must reject before source retirement or visibility mutation")
            }
        };
        assert!(matches!(
            item_error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                item_limit,
                selected_items,
                ..
            } if item_limit == required_items - 1 && selected_items == required_items
        ));
        assert!(first_source.exists());
        assert!(second_source.exists());
        assert!(replacement.marker_path.exists());
        assert_eq!(memory.current_bytes(), 0);

        let byte_error = match next_runtime_replacement_bounded(
            &mut cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            required_items,
            required_bytes - 1,
            |bytes| memory.reserve(bytes),
        ) {
            Err(err) => err,
            Ok(_) => panic!("one byte below the aggregate marker/root peak must reject"),
        };
        assert!(matches!(
            byte_error,
            TsinkError::MaintenanceWorkItemTooLarge {
                limit,
                required,
                ..
            } if limit == required_bytes - 1 && required == required_bytes
        ));
        assert!(first_source.exists());
        assert!(second_source.exists());
        assert!(replacement.marker_path.exists());
        assert_eq!(memory.current_bytes(), 0);

        let aggregate_memory_bytes = bounded_marker_cursor_reservation_bytes(&data_path)
            .saturating_add(marker_memory)
            .saturating_add(segment_memory);
        let below_global_memory =
            TestRecoveryMemory::with_budget(u64::try_from(aggregate_memory_bytes - 1).unwrap());
        let mut below_memory_cursor = BackgroundPostFlushRecoveryCursor::default();
        let memory_error = match next_runtime_replacement_bounded(
            &mut below_memory_cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            required_items,
            required_bytes,
            |bytes| below_global_memory.reserve(bytes),
        ) {
            Err(err) => err,
            Ok(_) => panic!("N-1 aggregate decode/transition memory must reject"),
        };
        assert!(matches!(
            memory_error,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == aggregate_memory_bytes - 1 && required == aggregate_memory_bytes
        ));
        assert_eq!(below_global_memory.current_bytes(), 0);
        assert!(first_source.exists());
        assert!(second_source.exists());
        assert!(replacement.marker_path.exists());

        let exact_global_memory =
            TestRecoveryMemory::with_budget(u64::try_from(aggregate_memory_bytes).unwrap());
        let mut exact_cursor = BackgroundPostFlushRecoveryCursor::default();
        let exact = next_runtime_replacement_bounded(
            &mut exact_cursor,
            &data_path,
            numeric_resolver(&numeric_lane),
            None,
            required_items,
            required_bytes,
            |bytes| exact_global_memory.reserve(bytes),
        )
        .expect("the exact aggregate marker/root envelope must admit");
        let BoundedRuntimeReplacementStep::Committing(exact) = exact else {
            panic!("expected a committing replacement");
        };
        assert_eq!(exact.selected_items(), required_items);
        assert_eq!(exact.selected_bytes(), required_bytes);
        assert_eq!(exact_global_memory.current_bytes(), aggregate_memory_bytes);
        drop(exact);
        exact_cursor.reset();
        assert_eq!(
            exact_global_memory.current_bytes(),
            0,
            "all retained cursor, marker, and segment decode memory must release"
        );
    }

    #[test]
    fn prepared_startup_recovery_rolls_back_a_published_output_and_clears_the_fence() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let staged_root = write_test_segment(&temp.path().join("staging"), 2);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();

        assert!(ensure_no_pending_post_flush_replacement(&data_path).is_err());
        finalize_startup(&data_path, &numeric_lane).unwrap();

        assert!(source_root.is_dir());
        assert!(!final_root.exists());
        assert!(!replacement.marker_path.exists());
        assert!(!output_rollback_path(&replacement, 0).unwrap().exists());
        ensure_no_pending_post_flush_replacement(&data_path).unwrap();
    }

    #[test]
    fn startup_marker_memory_rejection_preserves_every_marker_and_segment_until_exact_threshold() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let staged_root = write_test_segment(&temp.path().join("staging"), 2);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();
        let marker_dir = replacement_marker_dir(&data_path);
        for index in 0..512u32 {
            fs::write(marker_dir.join(format!("operator-{index:04x}")), b"keep").unwrap();
        }

        let marker_files = enumerate_startup_marker_files(&data_path, usize::MAX).unwrap();
        let configured_root_path_bytes = numeric_lane
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .saturating_add(64);
        let exact =
            startup_recovery_memory_upper_bound(&marker_files, configured_root_path_bytes).unwrap();
        let error = finalize_pending_post_flush_replacements_for_startup(
            &data_path,
            Some(&numeric_lane),
            None,
            None,
            None,
            exact - 1,
        )
        .expect_err("one byte below the complete startup recovery peak must reject");
        assert!(matches!(
            error,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == exact - 1 && required == exact
        ));
        assert!(source_root.is_dir());
        assert!(final_root.is_dir());
        assert!(replacement.marker_path.is_file());
        for index in 0..512u32 {
            assert!(marker_dir.join(format!("operator-{index:04x}")).is_file());
        }

        finalize_pending_post_flush_replacements_for_startup(
            &data_path,
            Some(&numeric_lane),
            None,
            None,
            None,
            exact,
        )
        .expect("the exact complete startup recovery peak must succeed");
        assert!(source_root.is_dir());
        assert!(!final_root.exists());
        assert!(!replacement.marker_path.exists());
        for index in 0..512u32 {
            assert_eq!(
                fs::read(marker_dir.join(format!("operator-{index:04x}"))).unwrap(),
                b"keep"
            );
        }
    }

    #[test]
    fn committing_startup_recovery_finishes_a_partially_retired_replacement() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let first_source = write_test_segment(&numeric_lane, 1);
        let second_source = write_test_segment(&numeric_lane, 2);
        let staged_root = write_test_segment(&temp.path().join("staging"), 3);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000003");
        let mut replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&first_source, true), source(&second_source, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();
        replacement
            .mark_committing(&data_path, numeric_resolver(&numeric_lane), None)
            .unwrap();
        let first_retired = source_retirement_path(&replacement, 0).unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&first_source, &first_retired).unwrap();

        assert!(ensure_no_pending_post_flush_replacement(&data_path).is_err());
        finalize_startup(&data_path, &numeric_lane).unwrap();

        assert!(!first_source.exists());
        assert!(!second_source.exists());
        assert!(!first_retired.exists());
        assert!(final_root.is_dir());
        assert!(!replacement.marker_path.exists());
        ensure_no_pending_post_flush_replacement(&data_path).unwrap();
    }

    #[test]
    fn committing_startup_recovery_supports_zero_output_expiration() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let mut replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, true)],
            &[],
            0,
            None,
        )
        .unwrap();
        replacement
            .mark_committing(&data_path, numeric_resolver(&numeric_lane), None)
            .unwrap();

        finalize_startup(&data_path, &numeric_lane).unwrap();

        assert!(!source_root.exists());
        assert!(!replacement.marker_path.exists());
    }

    fn assert_source_extra_is_rejected(
        add_extra: impl FnOnce(&Path, &Path),
    ) -> (TempDir, PathBuf, PathBuf) {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        add_extra(&source_root, temp.path());
        let error = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[],
            0,
            None,
        )
        .expect_err("a source with a non-format entry must be rejected before marker publication");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert!(source_root.is_dir());
        assert!(!replacement_marker_dir(&data_path).exists());
        (temp, source_root, data_path)
    }

    #[test]
    fn source_with_an_extra_file_is_rejected_without_deleting_it() {
        let (_temp, source_root, _data_path) = assert_source_extra_is_rejected(|root, _| {
            fs::write(root.join("operator-note"), b"keep").unwrap();
        });
        assert_eq!(
            fs::read(source_root.join("operator-note")).unwrap(),
            b"keep"
        );
    }

    #[test]
    fn source_with_an_extra_subdirectory_is_rejected_without_deleting_it() {
        let (_temp, source_root, _data_path) = assert_source_extra_is_rejected(|root, _| {
            fs::create_dir(root.join("operator-directory")).unwrap();
            fs::write(root.join("operator-directory").join("keep"), b"keep").unwrap();
        });
        assert_eq!(
            fs::read(source_root.join("operator-directory").join("keep")).unwrap(),
            b"keep"
        );
    }

    #[cfg(unix)]
    #[test]
    fn source_with_an_extra_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;

        let (temp, source_root, _data_path) = assert_source_extra_is_rejected(|root, temp| {
            let external = temp.join("external");
            fs::write(&external, b"outside").unwrap();
            symlink(&external, root.join("operator-link")).unwrap();
        });
        assert_eq!(fs::read(temp.path().join("external")).unwrap(), b"outside");
        assert!(fs::symlink_metadata(source_root.join("operator-link"))
            .unwrap()
            .file_type()
            .is_symlink());
    }

    fn assert_staged_output_extra_is_rejected(
        add_extra: impl FnOnce(&Path, &Path),
    ) -> (TempDir, PathBuf) {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let staged_root = write_test_segment(&temp.path().join("staging"), 2);
        add_extra(&staged_root, temp.path());
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let error = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .expect_err("a staged output with a non-format entry must fail before marker publication");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert!(source_root.is_dir());
        assert!(staged_root.is_dir());
        assert!(!final_root.exists());
        assert!(!replacement_marker_dir(&data_path).exists());
        (temp, staged_root)
    }

    #[test]
    fn staged_output_with_an_extra_file_is_rejected_without_deleting_it() {
        let (_temp, staged_root) = assert_staged_output_extra_is_rejected(|root, _| {
            fs::write(root.join("operator-note"), b"keep").unwrap();
        });
        assert_eq!(
            fs::read(staged_root.join("operator-note")).unwrap(),
            b"keep"
        );
    }

    #[test]
    fn staged_output_with_an_extra_subdirectory_is_rejected_without_deleting_it() {
        let (_temp, staged_root) = assert_staged_output_extra_is_rejected(|root, _| {
            fs::create_dir(root.join("operator-directory")).unwrap();
            fs::write(root.join("operator-directory").join("keep"), b"keep").unwrap();
        });
        assert_eq!(
            fs::read(staged_root.join("operator-directory").join("keep")).unwrap(),
            b"keep"
        );
    }

    #[cfg(unix)]
    #[test]
    fn staged_output_with_an_extra_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;

        let (temp, staged_root) = assert_staged_output_extra_is_rejected(|root, temp| {
            let external = temp.join("external-staged");
            fs::write(&external, b"outside").unwrap();
            symlink(&external, root.join("operator-link")).unwrap();
        });
        assert_eq!(
            fs::read(temp.path().join("external-staged")).unwrap(),
            b"outside"
        );
        assert!(fs::symlink_metadata(staged_root.join("operator-link"))
            .unwrap()
            .file_type()
            .is_symlink());
    }

    fn assert_final_output_extra_is_rejected(
        add_extra: impl FnOnce(&Path, &Path),
    ) -> (TempDir, PathBuf, PathBuf) {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let staged_root = write_test_segment(&temp.path().join("staging"), 2);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let mut replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();
        add_extra(&final_root, temp.path());
        let error = replacement
            .mark_committing(&data_path, numeric_resolver(&numeric_lane), None)
            .expect_err("a final output with a non-format entry must fail before the commit point");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert!(source_root.is_dir());
        assert!(final_root.is_dir());
        assert!(replacement.marker_path.is_file());
        (temp, final_root, replacement.marker_path)
    }

    #[test]
    fn final_output_with_an_extra_file_is_rejected_without_deleting_it() {
        let (_temp, final_root, marker_path) = assert_final_output_extra_is_rejected(|root, _| {
            fs::write(root.join("operator-note"), b"keep").unwrap();
        });
        assert_eq!(fs::read(final_root.join("operator-note")).unwrap(), b"keep");
        assert!(marker_path.is_file());
    }

    #[test]
    fn final_output_with_an_extra_subdirectory_is_rejected_without_deleting_it() {
        let (_temp, final_root, marker_path) = assert_final_output_extra_is_rejected(|root, _| {
            fs::create_dir(root.join("operator-directory")).unwrap();
            fs::write(root.join("operator-directory").join("keep"), b"keep").unwrap();
        });
        assert_eq!(
            fs::read(final_root.join("operator-directory").join("keep")).unwrap(),
            b"keep"
        );
        assert!(marker_path.is_file());
    }

    #[cfg(unix)]
    #[test]
    fn final_output_with_an_extra_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;

        let (temp, final_root, marker_path) =
            assert_final_output_extra_is_rejected(|root, temp| {
                let external = temp.join("external-final");
                fs::write(&external, b"outside").unwrap();
                symlink(&external, root.join("operator-link")).unwrap();
            });
        assert_eq!(
            fs::read(temp.path().join("external-final")).unwrap(),
            b"outside"
        );
        assert!(fs::symlink_metadata(final_root.join("operator-link"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(marker_path.is_file());
    }

    fn write_named_marker(data_path: &Path, id: &str, bytes: &[u8]) -> PathBuf {
        let marker_dir = replacement_marker_dir(data_path);
        fs::create_dir_all(&marker_dir).unwrap();
        let marker_path = marker_dir.join(format!(
            "{POST_FLUSH_REPLACEMENT_MARKER_PREFIX}{id}{POST_FLUSH_REPLACEMENT_MARKER_SUFFIX}"
        ));
        fs::write(&marker_path, bytes).unwrap();
        marker_path
    }

    #[test]
    fn oversized_and_corrupt_markers_fail_closed() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        fs::create_dir_all(&numeric_lane).unwrap();
        let corrupt =
            write_named_marker(&data_path, "0000000000000001-0000000000000001", b"not-json");
        assert!(parse_marker(&data_path, numeric_resolver(&numeric_lane), &corrupt).is_err());
        fs::remove_file(&corrupt).unwrap();

        let oversized = write_named_marker(
            &data_path,
            "0000000000000002-0000000000000002",
            &vec![b'x'; MAX_POST_FLUSH_REPLACEMENT_MARKER_BYTES as usize + 1],
        );
        let error = parse_marker(&data_path, numeric_resolver(&numeric_lane), &oversized)
            .expect_err("oversized marker must be rejected before an unbounded read");
        assert!(error.to_string().contains("exceeds"));
    }

    #[test]
    fn marker_record_count_is_bounded_before_path_resolution() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        fs::create_dir_all(&numeric_lane).unwrap();
        let id = "0000000000000001-0000000000000002";
        let repeated = PostFlushSourceRecord {
            segment: PostFlushSegmentRecord {
                lane: SegmentLaneFamily::Numeric,
                tier: PersistedSegmentTier::Hot,
                relative_path: "segments/L0/seg-0000000000000001".to_string(),
            },
            counts_as_expired: false,
        };
        let marker = PostFlushReplacementMarker {
            version: POST_FLUSH_REPLACEMENT_VERSION,
            phase: PostFlushReplacementPhase::Prepared,
            id: id.to_string(),
            sources: vec![repeated; MAX_POST_FLUSH_REPLACEMENT_RECORDS + 1],
            outputs: Vec::new(),
            tier_moves: 0,
        };
        let marker_path = write_named_marker(&data_path, id, &serde_json::to_vec(&marker).unwrap());
        let error = parse_marker(&data_path, numeric_resolver(&numeric_lane), &marker_path)
            .expect_err("too many records must be rejected");
        assert!(error.to_string().contains("record limit"));
    }

    #[test]
    fn initial_marker_capacity_includes_payload_marker_entry_and_missing_directory() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let segment = locate_segment_record(numeric_resolver(&numeric_lane), &source_root).unwrap();
        let marker = PostFlushReplacementMarker {
            version: POST_FLUSH_REPLACEMENT_VERSION,
            phase: PostFlushReplacementPhase::Prepared,
            id: "0000000000000000-0000000000000000".to_string(),
            sources: vec![PostFlushSourceRecord {
                segment: segment.record,
                counts_as_expired: false,
            }],
            outputs: Vec::new(),
            tier_moves: 0,
        };
        let payload_bytes = serde_json::to_vec(&marker).unwrap().len() as u64;
        let probe = crate::LocalDiskBudget::open(&data_path, LocalDiskLimits::default()).unwrap();
        let used = probe.snapshot().accounted_bytes;
        let allowance = probe
            .snapshot_restore_entry_staging_allowance_bytes()
            .unwrap();
        let expected_peak = payload_bytes + allowance * 2;
        drop(probe);
        let budget = crate::LocalDiskBudget::open(
            &data_path,
            LocalDiskLimits {
                max_bytes: Some(used + expected_peak - 1),
                ..LocalDiskLimits::default()
            },
        )
        .unwrap();

        let error = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[],
            0,
            Some(&budget),
        )
        .expect_err("the complete initial marker peak must be admitted before directory creation");
        assert!(matches!(
            error,
            TsinkError::InsufficientCompactionHeadroom { requested, .. }
                if requested == expected_peak
        ));
        assert!(!replacement_marker_dir(&data_path).exists());
    }

    #[test]
    fn prepared_rollback_zero_headroom_rejects_before_the_first_output_rename() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let staged_root = write_test_segment(&temp.path().join("staging"), 2);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();
        let budget = crate::LocalDiskBudget::open_with_available_space_for_test(
            &data_path,
            LocalDiskLimits::default(),
            0,
        )
        .unwrap();

        assert!(matches!(
            replacement.rollback_prepared(Some(&budget)),
            Err(TsinkError::InsufficientDiskSpace { .. })
        ));
        assert!(final_root.is_dir());
        assert!(source_root.is_dir());
        assert!(replacement.marker_path.is_file());
        assert!(!output_rollback_path(&replacement, 0).unwrap().exists());
    }

    #[test]
    fn committing_retirement_zero_headroom_rejects_before_the_first_source_rename() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_lane = data_path.join("lane_numeric");
        let source_root = write_test_segment(&numeric_lane, 1);
        let staged_root = write_test_segment(&temp.path().join("staging"), 2);
        let final_root = numeric_lane
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let mut replacement = publish_prepared_replacement(
            &data_path,
            numeric_resolver(&numeric_lane),
            &[source(&source_root, false)],
            &[promotion(&staged_root, &final_root)],
            0,
            None,
        )
        .unwrap();
        crate::engine::fs_utils::rename_and_sync_parents(&staged_root, &final_root).unwrap();
        replacement
            .mark_committing(&data_path, numeric_resolver(&numeric_lane), None)
            .unwrap();
        let budget = crate::LocalDiskBudget::open_with_available_space_for_test(
            &data_path,
            LocalDiskLimits::default(),
            0,
        )
        .unwrap();

        assert!(matches!(
            replacement.finish_committing(Some(&budget)),
            Err(TsinkError::InsufficientDiskSpace { .. })
        ));
        assert!(source_root.is_dir());
        assert!(final_root.is_dir());
        assert!(replacement.marker_path.is_file());
        assert!(!source_retirement_path(&replacement, 0).unwrap().exists());
    }
}
