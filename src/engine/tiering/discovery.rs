use std::path::Path;
use std::{fs::File, io::Read};

use tracing::warn;

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::inventory::SegmentInventoryAccumulator;
use super::layout::SegmentScanTarget;
use super::{SegmentInventory, SegmentInventoryEntry, SegmentPathResolver};
use crate::engine::binio::{FILE_FLAG_ZSTD_BODY, MAX_DECODED_FRAMED_FILE_BYTES};
use crate::engine::segment::{
    list_segment_dirs, quarantine_invalid_startup_segment, read_segment_manifest,
    read_segment_manifest_fingerprint, segment_validation_error, segment_validation_error_message,
    visit_segment_dirs_with_namespace_budget, SegmentValidationContext, StartupQuarantinedSegment,
};
use crate::engine::segment::{MAX_SEGMENT_CHUNKS_FILE_BYTES, MAX_SEGMENT_MANIFEST_FILE_BYTES};

// One encoded metadata byte can coexist with decoded tables, per-segment and global chunk refs,
// time-bucket/postings indexes, registry-rebuild state, and the final persisted-index structures.
// Sixteen is intentionally an upper-envelope model rather than an allocator-exact measurement.
const STARTUP_SEGMENT_METADATA_EXPANSION_FACTOR: usize = 16;
// Discovery temporarily holds scan roots, dedupe-map keys/values, lane root lists, and loaded-index
// roots. Admit all four path payload copies plus a fixed B-tree/vector bookkeeping allowance.
const STARTUP_SEGMENT_ROOT_BOOKKEEPING_WORDS: usize = 16;
const STARTUP_SEGMENT_ROOT_PATH_COPIES: usize = 4;

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct StartupRecoveredSegmentInventory {
    pub(in crate::engine::storage_engine) inventory: SegmentInventory,
    pub(in crate::engine::storage_engine) quarantined: Vec<StartupQuarantinedSegment>,
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_startup_recoverable(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<StartupRecoveredSegmentInventory> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();
    let mut quarantined = Vec::new();

    for target in resolver.inventory_scan_targets() {
        scan_startup_segment_roots_recoverable(&target, &mut deduped, &mut quarantined)?;
    }

    Ok(StartupRecoveredSegmentInventory {
        inventory: deduped.finish(),
        quarantined,
    })
}

/// Read-only conservative admission pass shared by every configured lane and tier.
///
/// This runs before compaction recovery or quarantine may rename durable state. Physical chunk
/// mapping lengths are charged once; metadata files receive an expansion allowance for decoded
/// tables, postings, registry rebuilds, and the eventual live persisted-index representation.
pub(in crate::engine::storage_engine) fn preflight_segment_inventory_startup_memory<Admit>(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
    mut admit: Admit,
) -> Result<()>
where
    Admit: FnMut(usize) -> Result<()>,
{
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
    );
    for target in resolver.inventory_scan_targets() {
        visit_segment_dirs_with_namespace_budget(
            &target.base_path,
            0..=2u8,
            &mut namespace_budget,
            |root| {
                let manifest_path = root.join("manifest.bin");
                let manifest_stored_bytes = match std::fs::metadata(&manifest_path) {
                    Ok(metadata) => usize::try_from(metadata.len()).unwrap_or(usize::MAX),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(err) => return Err(err.into()),
                };
                let root_reservation = std::mem::size_of::<SegmentInventoryEntry>()
                    .saturating_add(
                        root.as_os_str()
                            .len()
                            .saturating_mul(STARTUP_SEGMENT_ROOT_PATH_COPIES),
                    )
                    .saturating_add(
                        STARTUP_SEGMENT_ROOT_BOOKKEEPING_WORDS
                            .saturating_mul(std::mem::size_of::<usize>()),
                    );
                admit(root_reservation)?;
                if manifest_stored_bytes > MAX_SEGMENT_MANIFEST_FILE_BYTES {
                    return Ok(());
                }
                admit(manifest_stored_bytes.saturating_mul(2))?;

                let fingerprint = match read_segment_manifest_fingerprint(root) {
                    Ok(fingerprint) => fingerprint,
                    Err(err) if segment_validation_error_message(&err).is_some() => return Ok(()),
                    Err(err) if is_not_found_error(&err) => return Ok(()),
                    Err(err) => return Err(err),
                };
                if let Some(retained_and_decode) =
                    startup_segment_retained_and_decode_reservation(root, &fingerprint.files)?
                {
                    admit(retained_and_decode)?;
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn startup_segment_retained_and_decode_reservation(
    root: &Path,
    files: &[crate::engine::segment::SegmentFileFingerprint; 4],
) -> Result<Option<usize>> {
    if !files
        .iter()
        .zip([1u8, 2, 3, 4])
        .all(|(file, expected_kind)| file.kind == expected_kind)
    {
        return Ok(None);
    }

    let mut reservation = 0usize;
    for file in files {
        let file_name = match file.kind {
            1 => "chunks.bin",
            2 => "chunk_index.bin",
            3 => "series.bin",
            4 => "postings.bin",
            _ => return Ok(None),
        };
        let path = root.join(file_name);
        let mut opened = match File::open(&path) {
            Ok(opened) => opened,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let stored_u64 = opened.metadata()?.len();
        if stored_u64 != file.file_len {
            return Ok(None);
        }
        let stored = match usize::try_from(stored_u64) {
            Ok(stored) => stored,
            Err(_) => return Ok(None),
        };

        if file.kind == 1 {
            if stored > MAX_SEGMENT_CHUNKS_FILE_BYTES {
                return Ok(None);
            }
            reservation = reservation.saturating_add(stored);
            continue;
        }
        if stored > MAX_DECODED_FRAMED_FILE_BYTES {
            return Ok(None);
        }

        let mut prefix = [0u8; 12];
        let prefix_len = stored.min(prefix.len());
        opened.read_exact(&mut prefix[..prefix_len])?;
        let decoded = if prefix_len >= 8
            && u16::from_le_bytes([prefix[6], prefix[7]]) & FILE_FLAG_ZSTD_BODY != 0
        {
            if prefix_len < prefix.len() {
                return Ok(None);
            }
            let body_len =
                u32::from_le_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]) as usize;
            match 8usize.checked_add(body_len) {
                Some(decoded) if decoded <= MAX_DECODED_FRAMED_FILE_BYTES => decoded,
                _ => return Ok(None),
            }
        } else {
            stored
        };
        reservation = reservation
            .saturating_add(stored)
            .saturating_add(decoded.saturating_mul(STARTUP_SEGMENT_METADATA_EXPANSION_FACTOR));
    }
    Ok(Some(reservation))
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_runtime_strict(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<SegmentInventory> {
    build_segment_inventory_fail_on_invalid(
        numeric_lane_path,
        blob_lane_path,
        tiered_storage,
        SegmentValidationContext::RuntimeRefresh,
    )
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_fail_on_invalid(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
    context: SegmentValidationContext,
) -> Result<SegmentInventory> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();

    for target in resolver.inventory_scan_targets() {
        scan_segment_roots(&target, context, &mut deduped)?;
    }

    Ok(deduped.finish())
}

fn scan_segment_roots(
    target: &SegmentScanTarget,
    context: SegmentValidationContext,
    deduped: &mut SegmentInventoryAccumulator,
) -> Result<()> {
    for root in list_segment_dirs(&target.base_path)? {
        let manifest = match read_segment_manifest(&root) {
            Ok(manifest) => manifest,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory disappeared during tiered inventory scan; skipping"
                );
                continue;
            }
            Err(TsinkError::DataCorruption(msg)) => {
                return Err(segment_validation_error(&root, context, &msg));
            }
            Err(err) => return Err(err),
        };

        deduped.insert(SegmentInventoryEntry {
            lane: target.lane,
            tier: target.tier,
            root,
            manifest,
        });
    }

    Ok(())
}

fn scan_startup_segment_roots_recoverable(
    target: &SegmentScanTarget,
    deduped: &mut SegmentInventoryAccumulator,
    quarantined: &mut Vec<StartupQuarantinedSegment>,
) -> Result<()> {
    for root in list_segment_dirs(&target.base_path)? {
        let manifest = match read_segment_manifest(&root) {
            Ok(manifest) => manifest,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory disappeared during tiered inventory scan; skipping"
                );
                continue;
            }
            Err(err) => {
                let Some(details) = segment_validation_error_message(&err) else {
                    return Err(err);
                };
                quarantined.push(quarantine_invalid_startup_segment(&root, &details)?);
                continue;
            }
        };

        deduped.insert(SegmentInventoryEntry {
            lane: target.lane,
            tier: target.tier,
            root,
            manifest,
        });
    }

    Ok(())
}

fn is_not_found_error(err: &TsinkError) -> bool {
    matches!(err, TsinkError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound)
}
