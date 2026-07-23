use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use tracing::warn;

use crate::engine::fs_utils::{path_exists_no_follow, rename_and_sync_parents, stage_dir_path};
use crate::{Result, TsinkError};

use super::LoadedSegment;

pub(crate) const STARTUP_SEGMENT_QUARANTINE_PURPOSE: &str = "startup-segment-quarantine";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentLoadPolicy {
    SkipInvalid,
    FailOnInvalid(SegmentValidationContext),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentValidationContext {
    Startup,
    RuntimeRefresh,
    Compaction,
    Maintenance,
}

#[derive(Debug, Clone)]
pub(crate) struct QuarantinedSegmentRoot {
    pub(crate) path: PathBuf,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) sync_failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StartupQuarantinedSegment {
    pub(crate) original_root: PathBuf,
    pub(crate) quarantined: QuarantinedSegmentRoot,
    pub(crate) details: String,
}

pub(crate) fn segment_validation_error_message(err: &TsinkError) -> Option<String> {
    match err {
        TsinkError::DataCorruption(msg) | TsinkError::Compression(msg) => Some(msg.clone()),
        _ => None,
    }
}

pub(crate) fn segment_validation_error(
    segment_root: &Path,
    context: SegmentValidationContext,
    details: &str,
) -> TsinkError {
    let context_label = match context {
        SegmentValidationContext::Startup => "startup",
        SegmentValidationContext::RuntimeRefresh => "runtime refresh",
        SegmentValidationContext::Compaction => "compaction",
        SegmentValidationContext::Maintenance => "maintenance",
    };
    TsinkError::DataCorruption(format!(
        "persisted segment {} failed {context_label} validation: {}",
        segment_root.display(),
        details
    ))
}

fn validate_exact_segment_entries_no_follow(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|source| TsinkError::IoWithPath {
        path: root.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "persisted segment is link-like or not a directory: {}",
            root.display()
        )));
    }

    let expected_names = [
        "chunks.bin",
        "chunk_index.bin",
        "series.bin",
        "postings.bin",
        "manifest.bin",
    ];
    let expected = expected_names.into_iter().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for entry in fs::read_dir(root).map_err(|source| TsinkError::IoWithPath {
        path: root.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: root.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "persisted segment contains a non-UTF-8 entry: {}",
                entry.path().display()
            ))
        })?;
        if !observed.insert(name.to_string()) || !expected.contains(name) {
            return Err(TsinkError::DataCorruption(format!(
                "persisted segment contains an unowned entry: {}",
                entry.path().display()
            )));
        }
    }
    if observed.len() != expected.len() {
        return Err(TsinkError::DataCorruption(format!(
            "persisted segment does not contain exactly the five required files: {}",
            root.display()
        )));
    }

    for file_name in expected_names {
        let path = root.join(file_name);
        let metadata = fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "persisted segment required file is link-like or not regular: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Loads one immutable segment only after proving that the directory contains exactly Tsink's
/// five canonical regular files and that its manifest identity matches its intended final path.
///
/// The entry-shape check is repeated after decoding so a link or entry swap cannot silently pass
/// retirement/replacement ownership validation.
pub(crate) fn load_complete_segment_no_follow(
    root: &Path,
    expected_level: u8,
    expected_segment_id: u64,
    context: SegmentValidationContext,
) -> Result<LoadedSegment> {
    validate_exact_segment_entries_no_follow(root)
        .map_err(|err| segment_validation_error(root, context, &err.to_string()))?;
    let segment = super::load_segment(root)
        .map_err(|err| segment_validation_error(root, context, &err.to_string()))?;
    validate_exact_segment_entries_no_follow(root)
        .map_err(|err| segment_validation_error(root, context, &err.to_string()))?;
    if segment.manifest.level != expected_level
        || segment.manifest.segment_id != expected_segment_id
    {
        return Err(segment_validation_error(
            root,
            context,
            &format!(
                "segment identity mismatch: expected L{expected_level}/seg-{expected_segment_id:016x}, manifest=L{}/seg-{:016x}",
                segment.manifest.level, segment.manifest.segment_id
            ),
        ));
    }
    Ok(segment)
}

pub(crate) fn runtime_refresh_disappearance_error(
    segment_root: &Path,
    err: &TsinkError,
) -> TsinkError {
    TsinkError::Other(format!(
        "persisted segment {} disappeared during runtime refresh (transient disappearance race): {}",
        segment_root.display(),
        err
    ))
}

pub(crate) fn quarantine_segment_root(
    segment_root: &Path,
    purpose: &str,
) -> Result<QuarantinedSegmentRoot> {
    let quarantine_root = stage_dir_path(segment_root, purpose)?;
    match rename_and_sync_parents(segment_root, &quarantine_root) {
        Ok(()) => Ok(QuarantinedSegmentRoot {
            path: quarantine_root,
            sync_failed: false,
        }),
        Err(err) => {
            let quarantine_exists = path_exists_no_follow(&quarantine_root)?;
            let source_exists = path_exists_no_follow(segment_root)?;
            if quarantine_exists && !source_exists {
                Ok(QuarantinedSegmentRoot {
                    path: quarantine_root,
                    sync_failed: true,
                })
            } else {
                Err(err)
            }
        }
    }
}

pub(crate) fn quarantine_invalid_startup_segment(
    segment_root: &Path,
    details: &str,
) -> Result<StartupQuarantinedSegment> {
    let quarantined = quarantine_segment_root(segment_root, STARTUP_SEGMENT_QUARANTINE_PURPOSE)
        .map_err(|quarantine_err| {
            TsinkError::Other(format!(
                "failed to quarantine corrupt persisted segment {} during startup: {}; original validation error: {}",
                segment_root.display(),
                quarantine_err,
                details
            ))
        })?;
    warn!(
        path = %segment_root.display(),
        quarantine_path = %quarantined.path.display(),
        sync_failed = quarantined.sync_failed,
        error = %details,
        "Startup quarantined invalid persisted segment"
    );
    Ok(StartupQuarantinedSegment {
        original_root: segment_root.to_path_buf(),
        quarantined,
        details: details.to_string(),
    })
}

pub(crate) fn handle_segment_dir_load_error(
    dir: &Path,
    err: TsinkError,
    policy: SegmentLoadPolicy,
    disappeared_message: &'static str,
    invalid_message: &'static str,
) -> Result<()> {
    match err {
        err if is_not_found_error(&err) => {
            warn!(
                path = %dir.display(),
                error = %err,
                "{}",
                disappeared_message
            );
            Ok(())
        }
        err => {
            if let Some(msg) = segment_validation_error_message(&err) {
                match policy {
                    SegmentLoadPolicy::SkipInvalid => {
                        warn!(path = %dir.display(), error = %msg, "{}", invalid_message);
                        Ok(())
                    }
                    SegmentLoadPolicy::FailOnInvalid(context) => {
                        Err(segment_validation_error(dir, context, &msg))
                    }
                }
            } else {
                Err(err)
            }
        }
    }
}

pub(crate) fn is_not_found_error(err: &TsinkError) -> bool {
    matches!(err, TsinkError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound)
}
