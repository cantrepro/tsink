use std::path::{Path, PathBuf};

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::{PersistedSegmentTier, SegmentLaneFamily};
use crate::engine::fs_utils::{
    copy_dir_recursive, remove_path_if_exists_and_sync_parent, rename_and_sync_parents,
    stage_dir_path, sync_dir,
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

    let Some(parent) = destination_root.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "tiered segment destination has no parent directory: {}",
            destination_root.display()
        )));
    };
    std::fs::create_dir_all(parent)?;

    let staging = stage_dir_path(destination_root, TIER_DESTINATION_COPY_PURPOSE)?;
    copy_dir_recursive(source_root, &staging)?;
    sync_dir(&staging)?;
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
                let _ = remove_path_if_exists_and_sync_parent(destination_root);
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
