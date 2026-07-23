use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::{PersistedSegmentTier, SegmentLaneFamily};
use crate::engine::fs_utils::{
    is_link_or_reparse_point, remove_empty_dir_if_exists, remove_file_if_exists,
    rename_and_sync_parents, stage_dir_path, sync_dir, sync_parent_dir,
};
use crate::engine::segment::{
    quarantine_segment_root, verify_segment_fingerprint, SegmentContentFingerprint, SegmentManifest,
};

pub(crate) const TIER_DESTINATION_COPY_PURPOSE: &str = "tier-segment";
pub(crate) const TIER_DESTINATION_QUARANTINE_PURPOSE: &str = "tier-destination-quarantine";

#[derive(Debug, Clone, Copy)]
pub(in crate::engine::storage_engine) struct SegmentPathResolver<'a> {
    numeric_lane_path: Option<&'a Path>,
    blob_lane_path: Option<&'a Path>,
    tiered_storage: Option<&'a TieredStorageConfig>,
}

#[derive(Debug, Clone)]
pub(super) struct SegmentScanTarget {
    pub(super) base_path: PathBuf,
    pub(super) lane: SegmentLaneFamily,
    pub(super) tier: PersistedSegmentTier,
}

impl<'a> SegmentPathResolver<'a> {
    pub(in crate::engine::storage_engine) fn new(
        numeric_lane_path: Option<&'a Path>,
        blob_lane_path: Option<&'a Path>,
        tiered_storage: Option<&'a TieredStorageConfig>,
    ) -> Self {
        Self {
            numeric_lane_path,
            blob_lane_path,
            tiered_storage,
        }
    }

    pub(super) fn inventory_scan_targets(self) -> Vec<SegmentScanTarget> {
        let mut targets = Vec::new();
        self.push_hot_scan_target(
            &mut targets,
            SegmentLaneFamily::Numeric,
            self.numeric_lane_path,
        );
        self.push_hot_scan_target(&mut targets, SegmentLaneFamily::Blob, self.blob_lane_path);

        if let Some(config) = self.tiered_storage {
            for tier in [PersistedSegmentTier::Warm, PersistedSegmentTier::Cold] {
                targets.push(SegmentScanTarget {
                    base_path: config.lane_path(SegmentLaneFamily::Numeric, tier),
                    lane: SegmentLaneFamily::Numeric,
                    tier,
                });
                targets.push(SegmentScanTarget {
                    base_path: config.lane_path(SegmentLaneFamily::Blob, tier),
                    lane: SegmentLaneFamily::Blob,
                    tier,
                });
            }
        }

        targets
    }

    pub(in crate::engine::storage_engine) fn lane_root(
        self,
        lane: SegmentLaneFamily,
        tier: PersistedSegmentTier,
    ) -> Result<PathBuf> {
        self.lane_root_with_context(lane, tier, "")
    }

    pub(super) fn catalog_lane_root(
        self,
        lane: SegmentLaneFamily,
        tier: PersistedSegmentTier,
    ) -> Result<PathBuf> {
        self.lane_root_with_context(lane, tier, " for segment catalog load")
    }

    pub(in crate::engine::storage_engine) fn segment_root(
        self,
        lane: SegmentLaneFamily,
        tier: PersistedSegmentTier,
        manifest: &SegmentManifest,
    ) -> Result<PathBuf> {
        Ok(self
            .lane_root(lane, tier)?
            .join(relative_segment_path(manifest)))
    }

    fn push_hot_scan_target(
        self,
        targets: &mut Vec<SegmentScanTarget>,
        lane: SegmentLaneFamily,
        base_path: Option<&Path>,
    ) {
        let Some(base_path) = base_path.map(Path::to_path_buf).or_else(|| {
            self.tiered_storage
                .map(|config| config.lane_path(lane, PersistedSegmentTier::Hot))
        }) else {
            return;
        };

        targets.push(SegmentScanTarget {
            base_path,
            lane,
            tier: PersistedSegmentTier::Hot,
        });
    }

    fn lane_root_with_context(
        self,
        lane: SegmentLaneFamily,
        tier: PersistedSegmentTier,
        context: &'static str,
    ) -> Result<PathBuf> {
        match tier {
            PersistedSegmentTier::Hot => self.hot_lane_root(lane, context),
            PersistedSegmentTier::Warm | PersistedSegmentTier::Cold => self
                .tiered_storage
                .map(|config| config.lane_path(lane, tier))
                .ok_or_else(|| {
                    TsinkError::InvalidConfiguration(format!(
                        "tiered storage is not configured{context}"
                    ))
                }),
        }
    }

    fn hot_lane_root(self, lane: SegmentLaneFamily, context: &'static str) -> Result<PathBuf> {
        let configured = match lane {
            SegmentLaneFamily::Numeric => self.numeric_lane_path.map(Path::to_path_buf),
            SegmentLaneFamily::Blob => self.blob_lane_path.map(Path::to_path_buf),
        };

        configured
            .or_else(|| {
                self.tiered_storage
                    .map(|config| config.lane_path(lane, PersistedSegmentTier::Hot))
            })
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "{} hot lane path is not configured{context}",
                    lane_name(lane)
                ))
            })
    }
}

pub(super) fn relative_segment_path(manifest: &SegmentManifest) -> PathBuf {
    PathBuf::from("segments")
        .join(format!("L{}", manifest.level))
        .join(format!("seg-{:016x}", manifest.segment_id))
}

pub(in crate::engine::storage_engine) fn destination_segment_root(
    config: &TieredStorageConfig,
    lane: SegmentLaneFamily,
    tier: PersistedSegmentTier,
    manifest: &SegmentManifest,
) -> PathBuf {
    config
        .lane_path(lane, tier)
        .join(relative_segment_path(manifest))
}

pub(in crate::engine::storage_engine) fn move_segment_to_tier(
    source_root: &Path,
    destination_root: &Path,
) -> Result<()> {
    if destination_root == source_root {
        return Ok(());
    }

    ensure_destination_matches_or_copy(source_root, destination_root)
}

fn lane_name(lane: SegmentLaneFamily) -> &'static str {
    match lane {
        SegmentLaneFamily::Numeric => "numeric",
        SegmentLaneFamily::Blob => "blob",
    }
}

fn ensure_destination_matches_or_copy(source_root: &Path, destination_root: &Path) -> Result<()> {
    let source_fingerprint = verify_segment_fingerprint(source_root)?;
    if destination_root.exists() {
        return ensure_destination_matches_fingerprint(
            &source_fingerprint,
            source_root,
            destination_root,
        );
    }

    let copy_plan = plan_exact_segment_copy(source_root, &source_fingerprint)?;

    let Some(parent) = destination_root.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "tiered segment destination has no parent directory: {}",
            destination_root.display()
        )));
    };
    std::fs::create_dir_all(parent)?;

    let staging = stage_dir_path(destination_root, TIER_DESTINATION_COPY_PURPOSE)?;
    let copy_result = (|| -> Result<()> {
        std::fs::create_dir(&staging).map_err(|source| TsinkError::IoWithPath {
            path: staging.clone(),
            source,
        })?;
        for file in &copy_plan {
            copy_segment_file_bounded_exact(file, &staging)?;
        }
        sync_dir(&staging)
    })();
    if let Err(copy_err) = copy_result {
        let cleanup_result = remove_exact_segment_copy_directory(&staging, &copy_plan);
        return match cleanup_result {
            Ok(()) => Err(copy_err),
            Err(cleanup_err) => Err(TsinkError::Other(format!(
                "tier segment staging copy failed: {copy_err}; staging cleanup failed: {cleanup_err}"
            ))),
        };
    }
    if let Err(verification_err) =
        ensure_destination_matches_fingerprint(&source_fingerprint, source_root, &staging)
    {
        let cleanup_result = remove_exact_segment_copy_directory(&staging, &copy_plan);
        return match cleanup_result {
            Ok(()) => Err(verification_err),
            Err(cleanup_err) => Err(TsinkError::Other(format!(
                "tier segment staging verification failed: {verification_err}; staging cleanup failed: {cleanup_err}"
            ))),
        };
    }
    rename_and_sync_parents(&staging, destination_root)?;

    if let Err(err) =
        ensure_destination_matches_fingerprint(&source_fingerprint, source_root, destination_root)
    {
        let quarantine_result =
            quarantine_segment_root(destination_root, TIER_DESTINATION_QUARANTINE_PURPOSE);
        return match quarantine_result {
            Ok(quarantined) => Err(TsinkError::Other(format!(
                "copied tier move destination {} failed verification against source {} and was quarantined at {}: {}",
                destination_root.display(),
                source_root.display(),
                quarantined.path.display(),
                err
            ))),
            Err(quarantine_err) => {
                let _ = remove_exact_segment_copy_directory(destination_root, &copy_plan);
                Err(TsinkError::Other(format!(
                    "copied tier move destination {} failed verification against source {}: {}; quarantine failed: {}",
                    destination_root.display(),
                    source_root.display(),
                    err,
                    quarantine_err
                )))
            }
        };
    }

    Ok(())
}

fn remove_exact_segment_copy_directory(
    directory: &Path,
    copy_plan: &[SegmentCopyFile],
) -> Result<()> {
    for file in copy_plan {
        let path = directory.join(&file.file_name);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(TsinkError::IoWithPath { path, source }),
        };
        if metadata.file_type().is_dir() && !is_link_or_reparse_point(&metadata) {
            return Err(TsinkError::DataCorruption(format!(
                "tier segment copy cleanup entry changed into a directory: {}",
                path.display()
            )));
        }
        remove_file_if_exists(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
    }
    match remove_empty_dir_if_exists(directory) {
        Ok(true) => sync_parent_dir(directory),
        Ok(false) => Ok(()),
        Err(source) => Err(TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        }),
    }
}

#[derive(Debug)]
struct SegmentCopyFile {
    source: PathBuf,
    file_name: OsString,
    expected_len: u64,
}

fn plan_exact_segment_copy(
    source_root: &Path,
    fingerprint: &SegmentContentFingerprint,
) -> Result<Vec<SegmentCopyFile>> {
    let mut expected = BTreeMap::<OsString, Option<u64>>::from([
        (OsString::from("manifest.bin"), None),
        (
            OsString::from("chunks.bin"),
            Some(fingerprint.files[0].file_len),
        ),
        (
            OsString::from("chunk_index.bin"),
            Some(fingerprint.files[1].file_len),
        ),
        (
            OsString::from("series.bin"),
            Some(fingerprint.files[2].file_len),
        ),
        (
            OsString::from("postings.bin"),
            Some(fingerprint.files[3].file_len),
        ),
    ]);
    let entries = crate::engine::fs_utils::collect_directory_entries_bounded(
        source_root,
        expected.len(),
        "tier segment source validation",
    )?;
    let mut plan = Vec::with_capacity(expected.len());
    for entry in entries {
        let file_name = entry.file_name();
        let Some(expected_len) = expected.remove(&file_name) else {
            return Err(TsinkError::DataCorruption(format!(
                "tier segment source contains an unexpected entry: {}",
                entry.path().display()
            )));
        };
        let path = entry.path();
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })?;
        if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_file() {
            return Err(TsinkError::DataCorruption(format!(
                "tier segment source entry is link-like or not a regular file: {}",
                path.display()
            )));
        }
        let observed_len = metadata.len();
        if expected_len.is_some_and(|expected_len| expected_len != observed_len) {
            return Err(TsinkError::DataCorruption(format!(
                "tier segment source changed after fingerprinting: {}",
                path.display()
            )));
        }
        plan.push(SegmentCopyFile {
            source: path,
            file_name,
            expected_len: expected_len.unwrap_or(observed_len),
        });
    }
    if !expected.is_empty() {
        return Err(TsinkError::DataCorruption(format!(
            "tier segment source is missing expected files beneath {}",
            source_root.display()
        )));
    }
    plan.sort_by(|left, right| left.file_name.cmp(&right.file_name));
    Ok(plan)
}

fn copy_segment_file_bounded_exact(file: &SegmentCopyFile, staging: &Path) -> Result<()> {
    copy_segment_file_bounded_exact_with_before_open(file, staging, || {})
}

fn copy_segment_file_bounded_exact_with_before_open(
    file: &SegmentCopyFile,
    staging: &Path,
    before_open: impl FnOnce(),
) -> Result<()> {
    let metadata =
        std::fs::symlink_metadata(&file.source).map_err(|source| TsinkError::IoWithPath {
            path: file.source.clone(),
            source,
        })?;
    if is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() != file.expected_len
    {
        return Err(TsinkError::DataCorruption(format!(
            "tier segment source changed after copy admission: {}",
            file.source.display()
        )));
    }

    before_open();
    let mut source_options = std::fs::OpenOptions::new();
    source_options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        source_options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        source_options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut source_file =
        source_options
            .open(&file.source)
            .map_err(|source| TsinkError::IoWithPath {
                path: file.source.clone(),
                source,
            })?;
    let opened_metadata = source_file
        .metadata()
        .map_err(|source| TsinkError::IoWithPath {
            path: file.source.clone(),
            source,
        })?;
    if is_link_or_reparse_point(&opened_metadata)
        || !opened_metadata.file_type().is_file()
        || opened_metadata.len() != file.expected_len
    {
        return Err(TsinkError::DataCorruption(format!(
            "tier segment source changed while opening admitted file: {}",
            file.source.display()
        )));
    }

    let destination = staging.join(&file.file_name);
    let mut destination_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&destination)
        .map_err(|source| TsinkError::IoWithPath {
            path: destination.clone(),
            source,
        })?;
    let copied = {
        let mut bounded = (&mut source_file).take(file.expected_len);
        std::io::copy(&mut bounded, &mut destination_file).map_err(|source| {
            TsinkError::IoWithPath {
                path: destination.clone(),
                source,
            }
        })?
    };
    let mut growth_probe = [0u8; 1];
    let grew = source_file
        .read(&mut growth_probe)
        .map_err(|source| TsinkError::IoWithPath {
            path: file.source.clone(),
            source,
        })?
        != 0;
    if copied != file.expected_len || grew {
        return Err(TsinkError::DataCorruption(format!(
            "tier segment source changed during bounded copy: {}",
            file.source.display()
        )));
    }
    std::fs::set_permissions(&destination, opened_metadata.permissions()).map_err(|source| {
        TsinkError::IoWithPath {
            path: destination.clone(),
            source,
        }
    })?;
    destination_file
        .flush()
        .map_err(|source| TsinkError::IoWithPath {
            path: destination.clone(),
            source,
        })?;
    destination_file
        .sync_all()
        .map_err(|source| TsinkError::IoWithPath {
            path: destination,
            source,
        })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    #[test]
    fn bounded_segment_copy_does_not_follow_a_source_swapped_to_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("source.bin");
        let outside = temp_dir.path().join("outside.bin");
        let staging = temp_dir.path().join("staging");
        std::fs::write(&source, b"inside").unwrap();
        std::fs::write(&outside, b"bypass").unwrap();
        std::fs::create_dir(&staging).unwrap();
        let file = SegmentCopyFile {
            source: source.clone(),
            file_name: OsString::from("copied.bin"),
            expected_len: 6,
        };

        let err = copy_segment_file_bounded_exact_with_before_open(&file, &staging, || {
            std::fs::remove_file(&source).unwrap();
            symlink(&outside, &source).unwrap();
        })
        .expect_err("the no-follow source open must reject a swapped symlink");
        assert!(matches!(err, TsinkError::IoWithPath { .. }));
        assert!(!staging.join("copied.bin").exists());
        assert_eq!(std::fs::read(&outside).unwrap(), b"bypass");
    }
}

fn ensure_destination_matches_fingerprint(
    source_fingerprint: &SegmentContentFingerprint,
    source_root: &Path,
    destination_root: &Path,
) -> Result<()> {
    let destination_fingerprint = verify_segment_fingerprint(destination_root)
        .map_err(|err| map_destination_verification_error(destination_root, err))?;
    if &destination_fingerprint == source_fingerprint {
        return Ok(());
    }

    Err(TsinkError::InvalidConfiguration(format!(
        "tier move destination {} does not match source {}",
        destination_root.display(),
        source_root.display()
    )))
}

fn map_destination_verification_error(destination_root: &Path, err: TsinkError) -> TsinkError {
    match err {
        TsinkError::DataCorruption(message) => TsinkError::DataCorruption(format!(
            "tier move destination {} failed verification: {}",
            destination_root.display(),
            message
        )),
        TsinkError::Compression(message) => TsinkError::Compression(format!(
            "tier move destination {} failed verification: {}",
            destination_root.display(),
            message
        )),
        other => TsinkError::Other(format!(
            "tier move destination {} could not be verified: {}",
            destination_root.display(),
            other
        )),
    }
}
