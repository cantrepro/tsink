use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
use super::*;
use crate::engine::tombstone::TOMBSTONES_FILE_NAME;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;

const TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION: &str = "tombstone recovery snapshot page";
const TOMBSTONE_RECOVERY_SNAPSHOT_PAGE_BASE_BYTES: usize = 16 * 1024;
const TOMBSTONE_LANE_CONSTRUCTION_OPERATION: &str = "tombstone lane construction";
const TOMBSTONE_LANE_CONSTRUCTION_FIXED_BYTES: usize = 16 * 1024;
const TOMBSTONE_LANE_PATH_ALLOCATION_ALLOWANCE_BYTES: usize = 128;
const TOMBSTONE_LANE_SIMULTANEOUS_PATH_COPIES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TombstoneRecoverySnapshotProgress {
    Complete,
    More,
}

struct TombstoneRecoverySnapshotPage {
    updates: TombstoneMap,
    has_more: bool,
    modeled_bytes: usize,
}

fn ensure_bounded_tombstone_maintenance_memory(
    reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    byte_limit: u64,
    requested_bytes: usize,
) -> Result<()> {
    let required = u64::try_from(requested_bytes).unwrap_or(u64::MAX);
    if required > byte_limit {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
            limit: byte_limit,
            required,
        });
    }
    reservation.ensure(requested_bytes)
}

fn tombstone_recovery_snapshot_entry_bytes(ranges: &[tombstone::TombstoneRange]) -> usize {
    tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES.saturating_add(
        ranges
            .len()
            .saturating_mul(std::mem::size_of::<tombstone::TombstoneRange>()),
    )
}

#[cfg(test)]
fn collect_tombstone_recovery_snapshot_page(
    current: &TombstoneMap,
    after_series_id: Option<SeriesId>,
    item_limit: usize,
    byte_limit: u64,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneRecoverySnapshotPage> {
    let start = after_series_id.map_or(Unbounded, Excluded);
    let mut updates = TombstoneMap::new();
    let mut modeled_bytes = TOMBSTONE_RECOVERY_SNAPSHOT_PAGE_BASE_BYTES;
    let mut has_more = false;

    for (&series_id, ranges) in current.range((start, Unbounded)) {
        if updates.len() == item_limit {
            if updates.is_empty() {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
                    limit: 0,
                    required: 1,
                });
            }
            has_more = true;
            break;
        }

        let next_bytes =
            modeled_bytes.saturating_add(tombstone_recovery_snapshot_entry_bytes(ranges));
        let next_bytes_u64 = u64::try_from(next_bytes).unwrap_or(u64::MAX);
        if next_bytes_u64 > byte_limit {
            if updates.is_empty() {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
                    limit: byte_limit,
                    required: next_bytes_u64,
                });
            }
            has_more = true;
            break;
        }

        // Admit the ordered-map node and exact cloned range payload before either allocation.
        admit_memory(next_bytes)?;
        updates.insert(series_id, ranges.clone());
        modeled_bytes = next_bytes;
    }

    Ok(TombstoneRecoverySnapshotPage {
        updates,
        has_more,
        modeled_bytes,
    })
}

fn next_tombstone_series_id_from_sources(
    local: &TombstoneMap,
    remote: &tombstone::ImmutableTombstoneSnapshot,
    after_series_id: Option<SeriesId>,
) -> Option<SeriesId> {
    let start = after_series_id.map_or(Unbounded, Excluded);
    let mut next = local
        .range((start, Unbounded))
        .next()
        .map(|(&series_id, _)| series_id);
    for shard_index in 0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT {
        let candidate = remote
            .shard(shard_index)
            .range((start, Unbounded))
            .next()
            .map(|(&series_id, _)| series_id);
        next = match (next, candidate) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(series_id), None) | (None, Some(series_id)) => Some(series_id),
            (None, None) => None,
        };
    }
    next
}

/// Collects one globally ordered page from the mutable base and fixed remote overlay.
///
/// The 256-way next-key probe is a fixed cost per selected series. Only the selected range
/// vectors are cloned/unioned, after their page bytes have passed admission.
fn collect_tombstone_recovery_snapshot_page_from_sources(
    local: &TombstoneMap,
    remote: &tombstone::ImmutableTombstoneSnapshot,
    after_series_id: Option<SeriesId>,
    item_limit: usize,
    byte_limit: u64,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneRecoverySnapshotPage> {
    let mut updates = TombstoneMap::new();
    let mut modeled_bytes = TOMBSTONE_RECOVERY_SNAPSHOT_PAGE_BASE_BYTES;
    let mut after = after_series_id;

    loop {
        let Some(series_id) = next_tombstone_series_id_from_sources(local, remote, after) else {
            return Ok(TombstoneRecoverySnapshotPage {
                updates,
                has_more: false,
                modeled_bytes,
            });
        };
        if updates.len() == item_limit {
            if updates.is_empty() {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
                    limit: 0,
                    required: 1,
                });
            }
            return Ok(TombstoneRecoverySnapshotPage {
                updates,
                has_more: true,
                modeled_bytes,
            });
        }

        let local_ranges = local.get(&series_id).map(Vec::as_slice);
        let remote_ranges = remote.ranges(series_id);
        let range_count = local_ranges
            .map_or(0, |ranges| ranges.len())
            .saturating_add(remote_ranges.map_or(0, |ranges| ranges.len()));
        let next_bytes = modeled_bytes
            .saturating_add(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES)
            .saturating_add(
                range_count.saturating_mul(std::mem::size_of::<tombstone::TombstoneRange>()),
            );
        let next_bytes_u64 = u64::try_from(next_bytes).unwrap_or(u64::MAX);
        if next_bytes_u64 > byte_limit {
            if updates.is_empty() {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
                    limit: byte_limit,
                    required: next_bytes_u64,
                });
            }
            return Ok(TombstoneRecoverySnapshotPage {
                updates,
                has_more: true,
                modeled_bytes,
            });
        }
        admit_memory(next_bytes)?;
        let ranges = match (local_ranges, remote_ranges) {
            (None, None) => unreachable!("selected series must exist in one source"),
            (Some(ranges), None) | (None, Some(ranges)) => ranges.to_vec(),
            (Some(local), Some(remote)) => {
                tombstone::union_normalized_tombstone_ranges(local, remote)
            }
        };
        updates.insert(series_id, ranges);
        modeled_bytes = next_bytes;
        after = Some(series_id);
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstoneIndexContext<'a> {
    pub(in crate::engine::storage_engine) data_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) numeric_lane_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) blob_lane_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) tiered_storage:
        Option<&'a super::super::config::TieredStorageConfig>,
    pub(in crate::engine::storage_engine) runtime_mode: StorageRuntimeMode,
    pub(in crate::engine::storage_engine) local_disk_budget:
        Option<&'a Arc<crate::LocalDiskBudget>>,
}

impl<'a> TombstoneIndexContext<'a> {
    fn tombstone_index_lane_count(self, include_tiered_storage: bool) -> usize {
        usize::from(self.numeric_lane_path.is_some())
            .saturating_add(usize::from(self.blob_lane_path.is_some()))
            .saturating_add(
                usize::from(include_tiered_storage && self.tiered_storage.is_some())
                    .saturating_mul(6),
            )
    }

    /// Bounds the original lane vector, normalized clone, namespace-resolution clones, and
    /// validation temporaries without first allocating any lane-owned `PathBuf`.
    pub(in crate::engine::storage_engine) fn tombstone_lane_construction_upper_bound(
        self,
        include_tiered_storage: bool,
    ) -> usize {
        let mut path_bytes = 0usize;
        for path in [self.numeric_lane_path, self.blob_lane_path]
            .into_iter()
            .flatten()
        {
            let lane_root_bytes = path.as_os_str().as_encoded_bytes().len();
            let namespace_bytes = self
                .data_path
                .or_else(|| path.parent())
                .unwrap_or(path)
                .as_os_str()
                .as_encoded_bytes()
                .len();
            path_bytes = path_bytes
                .saturating_add(namespace_bytes)
                .saturating_add(lane_root_bytes)
                .saturating_add(TOMBSTONES_FILE_NAME.len())
                .saturating_add(TOMBSTONE_LANE_PATH_ALLOCATION_ALLOWANCE_BYTES);
        }
        if include_tiered_storage {
            if let Some(config) = self.tiered_storage {
                let root_bytes = config
                    .object_store_root
                    .as_os_str()
                    .as_encoded_bytes()
                    .len();
                // `hot|warm|cold` + `numeric|blob` + `tombstones.bin`, separators, and allocator
                // rounding are all dominated by this fixed suffix allowance.
                let manifest_bytes = root_bytes
                    .saturating_add(128)
                    .saturating_add(TOMBSTONE_LANE_PATH_ALLOCATION_ALLOWANCE_BYTES);
                path_bytes = path_bytes
                    .saturating_add(root_bytes.saturating_add(manifest_bytes).saturating_mul(6));
            }
        }
        TOMBSTONE_LANE_CONSTRUCTION_FIXED_BYTES
            .saturating_add(
                self.tombstone_index_lane_count(include_tiered_storage)
                    .saturating_mul(std::mem::size_of::<tombstone::TombstoneLane>())
                    .saturating_mul(3),
            )
            .saturating_add(path_bytes.saturating_mul(TOMBSTONE_LANE_SIMULTANEOUS_PATH_COPIES))
    }

    fn ensure_tombstone_lane_construction_bounded(
        self,
        include_tiered_storage: bool,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
        retained_bytes: usize,
    ) -> Result<()> {
        let requested = retained_bytes
            .saturating_add(self.tombstone_lane_construction_upper_bound(include_tiered_storage));
        let required = u64::try_from(requested).unwrap_or(u64::MAX);
        if required > byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: TOMBSTONE_LANE_CONSTRUCTION_OPERATION,
                limit: byte_limit,
                required,
            });
        }
        reservation.ensure(requested)
    }

    fn tombstone_index_lanes(self, include_tiered_storage: bool) -> Vec<tombstone::TombstoneLane> {
        let mut lanes = Vec::new();
        if let Some(path) = self.numeric_lane_path {
            lanes.push(tombstone::TombstoneLane {
                role: tombstone::TombstoneLaneRole::LocalNumeric,
                namespace_root: self
                    .data_path
                    .or_else(|| path.parent())
                    .unwrap_or(path)
                    .to_path_buf(),
                manifest_path: path.join(TOMBSTONES_FILE_NAME),
            });
        }
        if let Some(path) = self.blob_lane_path {
            lanes.push(tombstone::TombstoneLane {
                role: tombstone::TombstoneLaneRole::LocalBlob,
                namespace_root: self
                    .data_path
                    .or_else(|| path.parent())
                    .unwrap_or(path)
                    .to_path_buf(),
                manifest_path: path.join(TOMBSTONES_FILE_NAME),
            });
        }
        if include_tiered_storage {
            if let Some(config) = self.tiered_storage {
                for (tier, numeric_role, blob_role) in [
                    (
                        PersistedSegmentTier::Hot,
                        tombstone::TombstoneLaneRole::HotNumeric,
                        tombstone::TombstoneLaneRole::HotBlob,
                    ),
                    (
                        PersistedSegmentTier::Warm,
                        tombstone::TombstoneLaneRole::WarmNumeric,
                        tombstone::TombstoneLaneRole::WarmBlob,
                    ),
                    (
                        PersistedSegmentTier::Cold,
                        tombstone::TombstoneLaneRole::ColdNumeric,
                        tombstone::TombstoneLaneRole::ColdBlob,
                    ),
                ] {
                    lanes.push(tombstone::TombstoneLane {
                        role: numeric_role,
                        namespace_root: config.object_store_root.clone(),
                        manifest_path: config
                            .lane_path(SegmentLaneFamily::Numeric, tier)
                            .join(TOMBSTONES_FILE_NAME),
                    });
                    lanes.push(tombstone::TombstoneLane {
                        role: blob_role,
                        namespace_root: config.object_store_root.clone(),
                        manifest_path: config
                            .lane_path(SegmentLaneFamily::Blob, tier)
                            .join(TOMBSTONES_FILE_NAME),
                    });
                }
            }
        }
        lanes
    }

    fn tombstone_index_load_lanes(self) -> Vec<tombstone::TombstoneLane> {
        self.tombstone_index_lanes(true)
    }

    pub(in crate::engine::storage_engine) fn remote_tombstone_refresh_lanes(
        self,
    ) -> Result<Vec<tombstone::TombstoneLane>> {
        if self.runtime_mode != StorageRuntimeMode::ComputeOnly {
            return Err(TsinkError::InvalidConfiguration(
                "finite remote tombstone refresh requires compute-only storage".to_string(),
            ));
        }
        let lanes = tombstone::normalize_tombstone_lanes(
            &self
                .tombstone_index_load_lanes()
                .into_iter()
                .filter(|lane| lane.role.is_shared_remote())
                .collect::<Vec<_>>(),
        )?;
        if lanes.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "finite remote tombstone refresh requires shared tombstone lanes".to_string(),
            ));
        }
        tombstone::validate_tombstone_lanes(&lanes)?;
        Ok(lanes)
    }

    fn tombstone_index_persist_lanes(self) -> Vec<tombstone::TombstoneLane> {
        self.tombstone_index_lanes(self.runtime_mode != StorageRuntimeMode::ComputeOnly)
    }

    fn transaction_data_path(self) -> Result<&'a Path> {
        self.data_path.ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "durable tombstone publication requires a process-leased data path".to_string(),
            )
        })
    }

    fn has_transaction_coordinator_bounded(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
    ) -> Result<bool> {
        if self.tombstone_index_lane_count(self.runtime_mode != StorageRuntimeMode::ComputeOnly)
            == 0
        {
            return Ok(false);
        }
        let data_path = self.transaction_data_path()?;
        let path_bytes = data_path
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .saturating_add(tombstone::TOMBSTONE_TRANSACTION_DIR_NAME.len())
            .saturating_add(tombstone::TOMBSTONE_TRANSACTION_FILE_NAME.len())
            .saturating_add(TOMBSTONE_LANE_PATH_ALLOCATION_ALLOWANCE_BYTES)
            .saturating_mul(3)
            .saturating_add(4096);
        ensure_bounded_tombstone_maintenance_memory(reservation, byte_limit, path_bytes)?;
        let coordinator = data_path
            .join(tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
            .join(tombstone::TOMBSTONE_TRANSACTION_FILE_NAME);
        match std::fs::symlink_metadata(&coordinator) {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(TsinkError::IoWithPath {
                path: coordinator,
                source,
            }),
        }
    }

    fn has_durable_tombstone_state(self) -> Result<bool> {
        if let Some(data_path) = self.data_path {
            let coordinator = data_path
                .join(tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
                .join(tombstone::TOMBSTONE_TRANSACTION_FILE_NAME);
            if crate::engine::fs_utils::path_exists_no_follow(&coordinator)? {
                return Ok(true);
            }
        }
        for lane in self.tombstone_index_persist_lanes() {
            if crate::engine::fs_utils::path_exists_no_follow(&lane.manifest_path)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::engine::storage_engine) fn recover_pending_transaction(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<tombstone::TombstoneRecoveryOutcome> {
        let lane_bytes = self.tombstone_lane_construction_upper_bound(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
        );
        reservation.ensure(lane_bytes)?;
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(tombstone::TombstoneRecoveryOutcome::NoTransaction);
        }
        tombstone::recover_tombstone_transaction_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            self.local_disk_budget,
            |bytes| reservation.ensure(lane_bytes.saturating_add(bytes)),
        )
    }

    fn prepare_committed_reload_bounded(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        item_limit: usize,
        byte_limit: u64,
    ) -> Result<Option<tombstone::PreparedCommittedTombstoneReload>> {
        let lane_bytes = self.tombstone_lane_construction_upper_bound(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
        );
        self.ensure_tombstone_lane_construction_bounded(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
            reservation,
            byte_limit,
            0,
        )?;
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(None);
        }
        tombstone::prepare_committed_tombstone_reload_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            item_limit,
            byte_limit,
            u64::try_from(lane_bytes)
                .unwrap_or(u64::MAX)
                .saturating_mul(2),
            |bytes| {
                ensure_bounded_tombstone_maintenance_memory(
                    reservation,
                    byte_limit,
                    lane_bytes.saturating_add(bytes),
                )
            },
        )
    }

    fn recover_pending_transaction_bounded_with_retained(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        item_limit: usize,
        byte_limit: u64,
        retained_bytes: usize,
        preloaded_work_items: usize,
        preloaded_work_bytes: u64,
    ) -> Result<tombstone::TombstoneRecoveryOutcome> {
        let lane_bytes = self.tombstone_lane_construction_upper_bound(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
        );
        self.ensure_tombstone_lane_construction_bounded(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
            reservation,
            byte_limit,
            retained_bytes,
        )?;
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(tombstone::TombstoneRecoveryOutcome::NoTransaction);
        }
        // Recovery's cleanup enumerates every coordinator/lane namespace before its first
        // deletion. Repeat that dependency window read-only now and combine it with preload work
        // so an N-1 item/byte rejection cannot partially mutate manifests.
        let namespace_transient = retained_bytes
            .saturating_add(lane_bytes)
            .saturating_add(tombstone::MAX_TOMBSTONE_TRANSACTION_PATH_BYTES)
            .saturating_add(4096);
        ensure_bounded_tombstone_maintenance_memory(reservation, byte_limit, namespace_transient)?;
        tombstone::preflight_tombstone_recovery_namespace_work(
            self.transaction_data_path()?,
            &lanes,
            item_limit,
            byte_limit,
            preloaded_work_items,
            preloaded_work_bytes,
        )?;
        tombstone::recover_tombstone_transaction_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            self.local_disk_budget,
            |bytes| {
                ensure_bounded_tombstone_maintenance_memory(
                    reservation,
                    byte_limit,
                    retained_bytes
                        .saturating_add(lane_bytes)
                        .saturating_add(bytes),
                )
            },
        )
    }

    fn recover_pending_transaction_bounded(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
    ) -> Result<tombstone::TombstoneRecoveryOutcome> {
        let lane_bytes = self.tombstone_lane_construction_upper_bound(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
        );
        self.ensure_tombstone_lane_construction_bounded(
            self.runtime_mode != StorageRuntimeMode::ComputeOnly,
            reservation,
            byte_limit,
            0,
        )?;
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(tombstone::TombstoneRecoveryOutcome::NoTransaction);
        }
        tombstone::recover_tombstone_transaction_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            self.local_disk_budget,
            |bytes| {
                ensure_bounded_tombstone_maintenance_memory(
                    reservation,
                    byte_limit,
                    lane_bytes.saturating_add(bytes),
                )
            },
        )
    }

    pub(in crate::engine::storage_engine) fn read_tombstones_index(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<TombstoneMap> {
        let mut merged = TombstoneMap::new();
        let lanes = tombstone::normalize_tombstone_lanes(&self.tombstone_index_load_lanes())?;
        if lanes.is_empty() {
            return Ok(merged);
        }
        tombstone::validate_tombstone_lanes(&lanes)?;
        for lane in lanes {
            let live = ChunkStorage::tombstone_map_memory_usage_bytes(&merged);
            let loaded = tombstone::load_tombstones_with_memory_admission(
                &lane.manifest_path,
                |lane_peak| reservation.resize(live.saturating_add(lane_peak)),
            )?;
            let loaded_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&loaded);
            // The decoded lane map remains live while its entries are copied into newly allocated
            // ordered-map nodes. The old merged tree does not rehash, so only the destination
            // growth plus the one decoded source need simultaneous staging.
            reservation.resize(
                live.saturating_mul(2)
                    .saturating_add(loaded_bytes.saturating_mul(2))
                    .saturating_add(4096),
            )?;
            for (series_id, ranges) in loaded {
                for range in ranges {
                    tombstone::merge_tombstone_range(merged.entry(series_id).or_default(), range);
                }
            }
            reservation.resize(ChunkStorage::tombstone_map_memory_usage_bytes(&merged))?;
        }
        Ok(merged)
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index_updates(
        self,
        updates: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> tombstone::TombstonePersistenceResult<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(());
        }
        tombstone::persist_tombstone_updates_across_paths_with_disk_budget_outcome_and_memory_admission(
            self.transaction_data_path()
                .map_err(tombstone::TombstonePersistenceError::definitively_clean)?,
            &lanes,
            updates,
            self.local_disk_budget,
            |bytes| reservation.ensure(bytes),
        )
    }

    fn transaction_staging_memory_upper_bound_bounded(
        self,
        updates: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
    ) -> Result<usize> {
        tombstone::tombstone_transaction_staging_memory_upper_bound_with_admission(
            &self.tombstone_index_persist_lanes(),
            updates,
            |bytes| ensure_bounded_tombstone_maintenance_memory(reservation, byte_limit, bytes),
        )
    }

    fn persist_tombstones_index_updates_bounded(
        self,
        updates: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
    ) -> tombstone::TombstonePersistenceResult<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(());
        }
        tombstone::persist_tombstone_updates_across_paths_with_disk_budget_outcome_and_memory_admission(
            self.transaction_data_path()
                .map_err(tombstone::TombstonePersistenceError::definitively_clean)?,
            &lanes,
            updates,
            self.local_disk_budget,
            |bytes| {
                ensure_bounded_tombstone_maintenance_memory(
                    reservation,
                    byte_limit,
                    bytes,
                )
            },
        )
    }

    pub(in crate::engine::storage_engine) fn transaction_probe_memory_upper_bound(
        self,
    ) -> Result<usize> {
        tombstone::tombstone_transaction_probe_memory_upper_bound(
            &self.tombstone_index_persist_lanes(),
        )
    }

    pub(in crate::engine::storage_engine) fn transaction_staging_memory_upper_bound(
        self,
        updates: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<usize> {
        tombstone::tombstone_transaction_staging_memory_upper_bound_with_admission(
            &self.tombstone_index_persist_lanes(),
            updates,
            |bytes| reservation.ensure(bytes),
        )
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index_snapshot_for_recovery(
        self,
        snapshot: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(());
        }
        match tombstone::persist_tombstone_snapshot_transactionally_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            snapshot,
            self.local_disk_budget,
            |bytes| reservation.ensure(bytes),
        ) {
            Ok(()) => Ok(()),
            Err(error) if error.is_committed() => {
                let error = error.into_tsink_error();
                tracing::warn!(
                    error = %error,
                    "Committed tombstone recovery snapshot left durable coordinator recovery debt"
                );
                Ok(())
            }
            Err(error) => Err(error.into_tsink_error()),
        }
    }

    fn persist_tombstones_index_snapshot_for_recovery_bounded(
        self,
        snapshot: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
    ) -> tombstone::TombstonePersistenceResult<()> {
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(());
        }
        tombstone::persist_tombstone_snapshot_transactionally_with_memory_admission(
            self.transaction_data_path()
                .map_err(tombstone::TombstonePersistenceError::definitively_clean)?,
            &lanes,
            snapshot,
            self.local_disk_budget,
            |bytes| ensure_bounded_tombstone_maintenance_memory(reservation, byte_limit, bytes),
        )
    }

    pub(in crate::engine::storage_engine) fn ensure_delete_tombstone_persistence_supported(
        self,
    ) -> Result<()> {
        if self.runtime_mode == StorageRuntimeMode::ComputeOnly {
            return Err(TsinkError::UnsupportedOperation {
                operation: "delete_series",
                reason: "compute-only storage mode cannot durably persist delete tombstones; send the request to a read-write node".to_string(),
            });
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstoneReadContext<'a> {
    pub(in crate::engine::storage_engine) tombstones: &'a RwLock<TombstoneMap>,
    pub(in crate::engine::storage_engine) remote_tombstones:
        &'a RwLock<Arc<tombstone::ImmutableTombstoneSnapshot>>,
}

impl<'a> TombstoneReadContext<'a> {
    pub(in crate::engine::storage_engine) fn snapshot(self) -> TombstoneMap {
        let mut snapshot = self.tombstones.read().clone();
        let remote = self.remote_tombstones.read();
        for shard_index in 0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT {
            for (&series_id, ranges) in remote.shard(shard_index).iter() {
                tombstone::merge_normalized_tombstone_ranges(
                    snapshot.entry(series_id).or_default(),
                    ranges,
                );
            }
        }
        snapshot
    }

    pub(in crate::engine::storage_engine) fn is_empty(self) -> bool {
        let local = self.tombstones.read();
        let remote = self.remote_tombstones.read();
        local.is_empty() && remote.is_empty()
    }

    pub(in crate::engine::storage_engine) fn memory_usage_upper_bound(self) -> usize {
        let local = self.tombstones.read();
        let remote = self.remote_tombstones.read();
        ChunkStorage::tombstone_map_memory_usage_bytes(&local)
            .saturating_add(remote.memory_usage_bytes())
    }

    pub(in crate::engine::storage_engine) fn max_tombstoned_series_id(self) -> Option<SeriesId> {
        self.tombstones
            .read()
            .keys()
            .next_back()
            .copied()
            .max(self.remote_tombstones.read().max_series_id())
    }

    pub(in crate::engine::storage_engine) fn with_series_tombstone_range_sources<R>(
        self,
        series_id: SeriesId,
        f: impl FnOnce(Option<&[TombstoneRange]>, Option<&[TombstoneRange]>) -> R,
    ) -> R {
        let local = self.tombstones.read();
        let remote = self.remote_tombstones.read();
        let local_ranges = local.get(&series_id).map(Vec::as_slice);
        let remote_ranges = remote.ranges(series_id);
        f(local_ranges, remote_ranges)
    }

    pub(in crate::engine::storage_engine) fn with_series_tombstone_ranges_for_query<R>(
        self,
        series_id: SeriesId,
        execution: Option<&QueryExecution>,
        f: impl FnOnce(Option<&[TombstoneRange]>) -> Result<R>,
    ) -> Result<R> {
        self.with_series_tombstone_range_sources(
            series_id,
            |local_ranges, remote_ranges| -> Result<R> {
                match (local_ranges, remote_ranges) {
                    (None, None) => f(None),
                    (Some(ranges), None) | (None, Some(ranges)) => f(Some(ranges)),
                    (Some(local_ranges), Some(remote_ranges)) => {
                        let range_count = local_ranges.len().saturating_add(remote_ranges.len());
                        let bytes = super::super::query_exec::modeled_vec_capacity_bytes::<
                            TombstoneRange,
                        >(range_count);
                        let _reservation = if let Some(execution) = execution {
                            execution.observe_intermediate_vector_size(
                                u64::try_from(range_count).unwrap_or(u64::MAX),
                            )?;
                            Some(execution.reserve_memory(bytes).map_err(TsinkError::from)?)
                        } else {
                            None
                        };
                        let merged = tombstone::union_normalized_tombstone_ranges(
                            local_ranges,
                            remote_ranges,
                        );
                        f(Some(&merged))
                    }
                }
            },
        )
    }

    pub(in crate::engine::storage_engine) fn remote_snapshot(
        self,
    ) -> Arc<tombstone::ImmutableTombstoneSnapshot> {
        Arc::clone(&self.remote_tombstones.read())
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstonePublicationContext<'a> {
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
    pub(in crate::engine::storage_engine) tombstones: &'a RwLock<TombstoneMap>,
    pub(in crate::engine::storage_engine) remote_tombstones:
        &'a RwLock<Arc<tombstone::ImmutableTombstoneSnapshot>>,
    pub(in crate::engine::storage_engine) tombstone_used_bytes: &'a AtomicU64,
}

impl<'a> TombstonePublicationContext<'a> {
    fn reserve_series_ids_referenced_by_tombstones(self, tombstones: &TombstoneMap) -> Result<()> {
        let Some(max_series_id) = tombstones.keys().copied().max() else {
            return Ok(());
        };
        self.registry.write().reserve_series_id(max_series_id)
    }

    pub(in crate::engine::storage_engine) fn publish_remote_tombstones_locked(
        self,
        storage: &ChunkStorage,
        candidate: Arc<tombstone::ImmutableTombstoneSnapshot>,
    ) -> Result<u64> {
        let current_epoch = storage.remote_tombstone_epoch();
        let next_epoch = current_epoch.checked_add(1).ok_or_else(|| {
            TsinkError::Other(
                "remote tombstone visibility epoch exhausted before publication".to_string(),
            )
        })?;
        if let Some(max_series_id) = candidate.max_series_id() {
            self.registry.write().reserve_series_id(max_series_id)?;
        }

        // Cache readers retain a read guard on this tag map for their whole scalar decision.
        // Taking the write side makes the pointer/epoch transition atomic to them without
        // enumerating, clearing, or rebuilding any cache entry.
        let _cache_epoch_guard = storage.visibility.series_visibility_cache_epochs.write();
        let mut remote = self.remote_tombstones.write();
        storage.with_included_memory_delta(
            self.tombstone_used_bytes,
            &mut remote,
            |snapshot| snapshot.memory_usage_bytes(),
            |snapshot| **snapshot = candidate,
        );
        storage
            .visibility
            .remote_tombstone_epoch
            .store(next_epoch, Ordering::Release);
        // Existing cache payloads remain allocated, but their per-series epoch tags no longer
        // match. The aggregate bounded timestamp deliberately remains a monotonic upper bound:
        // retaining a stale value can conservatively reject an additional old write, whereas
        // resetting it could admit an out-of-retention write while unaffected future-bounded
        // series still exist. Lazy current-epoch rebuilds may raise this upper bound without
        // enumerating stale physical cache entries.
        storage.bump_live_series_pruning_generation();
        storage.bump_tombstone_state_generation();
        storage.bump_visibility_state_generation();
        Ok(storage.visibility_state_generation())
    }

    pub(in crate::engine::storage_engine) fn timestamp_survives_tombstones(
        timestamp: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> bool {
        !tombstone_ranges
            .is_some_and(|ranges| tombstone::timestamp_is_tombstoned(timestamp, ranges))
    }

    pub(in crate::engine::storage_engine) fn admit_loaded_tombstone_publication(
        self,
        storage: &ChunkStorage,
        merged: &TombstoneMap,
        transition_visibility_headroom: usize,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let merged_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(merged);
        let changed_count = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .count()
        };
        if changed_count == 0 {
            return reservation.ensure(merged_bytes.saturating_add(transition_visibility_headroom));
        }
        let changed_set_upper_bound = changed_count.saturating_mul(128).saturating_add(4096);
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(transition_visibility_headroom),
        )?;
        let changed_series_ids = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(changed_series_ids.iter());
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(visibility_staging)
                .saturating_add(transition_visibility_headroom),
        )?;
        self.reserve_series_ids_referenced_by_tombstones(merged)
    }

    fn admit_loaded_tombstone_publication_bounded(
        self,
        storage: &ChunkStorage,
        merged: &TombstoneMap,
        transition_visibility_headroom: usize,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
        byte_limit: u64,
    ) -> Result<()> {
        let merged_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(merged);
        let changed_count = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .count()
        };
        let changed_set_upper_bound = changed_count.saturating_mul(128).saturating_add(4096);
        ensure_bounded_tombstone_maintenance_memory(
            reservation,
            byte_limit,
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(transition_visibility_headroom),
        )?;
        if changed_count == 0 {
            return Ok(());
        }
        let changed_series_ids = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(changed_series_ids.iter());
        ensure_bounded_tombstone_maintenance_memory(
            reservation,
            byte_limit,
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(visibility_staging)
                .saturating_add(transition_visibility_headroom),
        )?;
        self.reserve_series_ids_referenced_by_tombstones(merged)
    }

    pub(in crate::engine::storage_engine) fn replace_loaded_tombstones_index(
        self,
        storage: &ChunkStorage,
        merged: TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let _visibility_guard = storage.visibility_write_fence();
        self.replace_loaded_tombstones_index_locked(storage, merged, reservation)
    }

    pub(in crate::engine::storage_engine) fn replace_loaded_tombstones_index_locked(
        self,
        storage: &ChunkStorage,
        merged: TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let merged_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&merged);
        let changed_count = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .count()
        };
        if changed_count == 0 {
            return Ok(());
        }
        let changed_set_upper_bound = changed_count.saturating_mul(128).saturating_add(4096);
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(4096),
        )?;

        let changed_series_ids = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(changed_series_ids.iter());
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(visibility_staging),
        )?;

        // The registry and visible tombstone/cache state remain untouched until every
        // publication allocation has passed admission.
        self.reserve_series_ids_referenced_by_tombstones(&merged)?;

        let mut tombstones = self.tombstones.write();
        storage.with_included_memory_delta(
            self.tombstone_used_bytes,
            &mut tombstones,
            |tombstones| ChunkStorage::tombstone_map_memory_usage_bytes(tombstones),
            |tombstones| **tombstones = merged,
        );
        drop(tombstones);
        #[cfg(test)]
        storage.invoke_tombstone_post_swap_pre_visibility_hook();
        #[cfg(test)]
        let refresh_result = storage
            .invoke_tombstone_post_commit_error_hook()
            .and_then(|()| {
                storage.refresh_series_visible_timestamp_cache_locked(
                    changed_series_ids.iter().copied(),
                )
            });
        #[cfg(not(test))]
        let refresh_result = storage
            .refresh_series_visible_timestamp_cache_locked(changed_series_ids.iter().copied());
        if let Err(err) = refresh_result {
            // The authoritative map has already swapped. Clear every affected summary before
            // returning success so readers rebuild from the new tombstone state; a retry may see
            // no map diff and therefore cannot be relied on to repair stale cache entries.
            storage.clear_series_visible_timestamp_cache(changed_series_ids.iter().copied());
            tracing::warn!(
                error = %err,
                "Tombstone reload deferred series visibility summary rebuild"
            );
        }
        storage.bump_tombstone_state_generation();
        storage.bump_visibility_state_generation();
        Ok(())
    }

    // Caller must hold the visibility write fence so rollup invalidation and tombstone
    // publication become visible to readers as one transition.
    pub(in crate::engine::storage_engine) fn publish_tombstone_updates_locked(
        self,
        storage: &ChunkStorage,
        index: TombstoneIndexContext<'a>,
        updates: TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> tombstone::TombstonePersistenceResult<()> {
        let update_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&updates);
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(updates.keys());
        let changed_set_upper_bound = updates.len().saturating_mul(128).saturating_add(4096);
        // Consuming the staged update tree can retain its source nodes while allocating new live
        // B-tree nodes. The live map itself is already included in `used_bytes` and never rehashes.
        let live_tree_growth_headroom = update_bytes;
        reservation
            .ensure(
                update_bytes
                    .saturating_add(visibility_staging)
                    .saturating_add(changed_set_upper_bound)
                    .saturating_add(live_tree_growth_headroom),
            )
            .map_err(tombstone::TombstonePersistenceError::definitively_clean)?;
        // Reserve the registry high-water mark before the durable commit boundary. The only
        // possible failure is series-id exhaustion; reporting that after manifest publication
        // would incorrectly present an effective delete as rejected.
        self.reserve_series_ids_referenced_by_tombstones(&updates)
            .map_err(tombstone::TombstonePersistenceError::definitively_clean)?;
        storage
            .validate_shared_object_store_writer_lock()
            .map_err(tombstone::TombstonePersistenceError::definitively_clean)?;
        match index.persist_tombstones_index_updates(&updates, reservation) {
            Ok(()) => {}
            Err(error) if error.is_committed() => {
                let error = error.into_tsink_error();
                tracing::warn!(
                    error = %error,
                    "Committed tombstone transaction left durable coordinator recovery debt"
                );
            }
            Err(error) => return Err(error),
        }
        self.apply_tombstone_updates_locked(storage, updates)
            .map_err(tombstone::TombstonePersistenceError::indeterminate)
    }

    fn apply_tombstone_updates_locked(
        self,
        storage: &ChunkStorage,
        updates: TombstoneMap,
    ) -> Result<()> {
        let changed_series_ids = updates.keys().copied().collect::<BTreeSet<_>>();
        let mut tombstones = self.tombstones.write();
        let fixed_bytes_before = tombstones
            .len()
            .saturating_mul(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES);
        let payload_bytes_before = updates.keys().fold(0usize, |bytes, series_id| {
            bytes.saturating_add(tombstones.get(series_id).map_or(0, |ranges| {
                ranges
                    .capacity()
                    .saturating_mul(std::mem::size_of::<tombstone::TombstoneRange>())
            }))
        });
        for (series_id, ranges) in updates {
            if ranges.is_empty() {
                tombstones.remove(&series_id);
            } else {
                tombstones.insert(series_id, ranges);
            }
        }
        let fixed_bytes_after = tombstones
            .len()
            .saturating_mul(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES);
        let payload_bytes_after = changed_series_ids.iter().fold(0usize, |bytes, series_id| {
            bytes.saturating_add(tombstones.get(series_id).map_or(0, |ranges| {
                ranges
                    .capacity()
                    .saturating_mul(std::mem::size_of::<tombstone::TombstoneRange>())
            }))
        });
        storage.account_included_memory_component_delta_bytes(
            self.tombstone_used_bytes,
            fixed_bytes_before.saturating_add(payload_bytes_before),
            fixed_bytes_after.saturating_add(payload_bytes_after),
        );
        drop(tombstones);
        #[cfg(test)]
        storage.invoke_tombstone_post_swap_pre_visibility_hook();
        #[cfg(test)]
        let refresh_result = storage
            .invoke_tombstone_post_commit_error_hook()
            .and_then(|()| {
                storage.refresh_series_visible_timestamp_cache_locked(
                    changed_series_ids.iter().copied(),
                )
            });
        #[cfg(not(test))]
        let refresh_result = storage
            .refresh_series_visible_timestamp_cache_locked(changed_series_ids.iter().copied());
        if let Err(err) = refresh_result {
            // The disk manifests and tombstone map have committed. Drop stale summaries so later
            // readers either rebuild them or surface the underlying storage error, while this
            // delete retains honest committed-success semantics.
            storage.clear_series_visible_timestamp_cache(changed_series_ids.iter().copied());
            tracing::warn!(
                error = %err,
                "Committed delete deferred series visibility summary rebuild"
            );
        }
        storage.bump_tombstone_state_generation();
        storage.bump_visibility_state_generation();
        Ok(())
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn recover_and_reload_tombstones_locked(
        &self,
    ) -> Result<bool> {
        self.recover_and_reload_tombstones_locked_with_limits(
            self.runtime.maintenance_max_items_per_pass,
            self.runtime.maintenance_max_bytes_per_pass,
        )
    }

    pub(in crate::engine::storage_engine) fn recover_and_reload_tombstones_locked_with_limits(
        &self,
        item_limit: usize,
        byte_limit: u64,
    ) -> Result<bool> {
        self.validate_shared_object_store_writer_lock()?;
        let index = self.tombstone_index_context();
        let mut reservation = self.tombstone_memory_reservation();
        // The ordinary finite publication probe must remain O(1) when there is no transaction:
        // admit only the coordinator path before constructing any lane vector or cloned paths.
        if !index.has_transaction_coordinator_bounded(&mut reservation, byte_limit)? {
            return Ok(false);
        }
        let mut prepared =
            index.prepare_committed_reload_bounded(&mut reservation, item_limit, byte_limit)?;
        let retained_bytes = prepared.as_ref().map_or(0, |prepared| {
            ChunkStorage::tombstone_map_memory_usage_bytes(&prepared.tombstones)
        });
        if prepared.is_none() {
            // A Prepared coordinator is not an authoritative delete decision. Segment visibility
            // may safely proceed without mutating it; the next delete/startup recovery owns its
            // bounded rollback. The safety-critical ordinary publication probe only rolls forward
            // a preloaded Committing candidate.
            return Ok(false);
        }
        if let Some(prepared) = prepared.as_ref() {
            debug_assert!(prepared.work_items <= item_limit);
            debug_assert!(prepared.work_bytes <= byte_limit);
            self.tombstone_publication_context()
                .admit_loaded_tombstone_publication_bounded(
                    self,
                    &prepared.tombstones,
                    prepared.recovery_memory_upper_bound,
                    &mut reservation,
                    byte_limit,
                )?;
        }
        let recovery = match index.recover_pending_transaction_bounded_with_retained(
            &mut reservation,
            item_limit,
            byte_limit,
            retained_bytes,
            prepared.as_ref().map_or(0, |prepared| prepared.work_items),
            prepared.as_ref().map_or(0, |prepared| prepared.work_bytes),
        ) {
            Ok(recovery) => recovery,
            Err(recovery_error) => {
                if let Some(prepared) = prepared.take() {
                    // A durable Committing record is itself the decision boundary. Even if a
                    // later lane rename/fsync fails, publish the already preadmitted candidate
                    // map so same-process readers cannot retain predecessor visibility while
                    // durable lanes are partially rolled forward.
                    self.tombstone_publication_context()
                        .replace_loaded_tombstones_index_locked(
                            self,
                            prepared.tombstones,
                            &mut reservation,
                        )?;
                }
                return Err(recovery_error);
            }
        };
        if !recovery.requires_authoritative_reload() {
            if prepared.is_some() {
                return Err(TsinkError::DataCorruption(
                    "committed tombstone reload preflight no longer matched the coordinator phase"
                        .to_string(),
                ));
            }
            return Ok(false);
        }
        let merged = prepared
            .take()
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "committed tombstone recovery lacked a preloaded authoritative map".to_string(),
                )
            })?
            .tombstones;
        self.tombstone_publication_context()
            .replace_loaded_tombstones_index_locked(self, merged, &mut reservation)?;
        Ok(true)
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn committed_tombstone_reload_preflight_bytes_for_tests(
        &self,
    ) -> Result<usize> {
        self.validate_shared_object_store_writer_lock()?;
        let index = self.tombstone_index_context();
        let mut reservation = self.tombstone_memory_reservation();
        let prepared = index
            .prepare_committed_reload_bounded(&mut reservation, usize::MAX, u64::MAX)?
            .ok_or_else(|| {
                TsinkError::Other(
                    "test expected a durable Committing tombstone coordinator".to_string(),
                )
            })?;
        self.tombstone_publication_context()
            .admit_loaded_tombstone_publication_bounded(
                self,
                &prepared.tombstones,
                prepared.recovery_memory_upper_bound,
                &mut reservation,
                u64::MAX,
            )?;
        Ok((self
            .memory
            .tombstone_staged_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize)
            .max(usize::try_from(prepared.work_bytes).unwrap_or(usize::MAX)))
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn recover_and_reload_tombstones_with_byte_limit_for_tests(
        &self,
        byte_limit: u64,
    ) -> Result<bool> {
        self.recover_and_reload_tombstones_locked_with_limits(usize::MAX, byte_limit)
    }

    pub(in crate::engine::storage_engine) fn tombstone_index_context(
        &self,
    ) -> TombstoneIndexContext<'_> {
        TombstoneIndexContext {
            data_path: self
                .persisted
                .series_index_path
                .as_deref()
                .and_then(Path::parent),
            numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
            blob_lane_path: self.persisted.blob_lane_path.as_deref(),
            tiered_storage: self.persisted.tiered_storage.as_ref(),
            runtime_mode: self.runtime.runtime_mode,
            local_disk_budget: self.persisted.local_disk_budget.as_ref(),
        }
    }

    pub(in crate::engine::storage_engine) fn tombstone_read_context(
        &self,
    ) -> TombstoneReadContext<'_> {
        TombstoneReadContext {
            tombstones: &self.visibility.tombstones,
            remote_tombstones: &self.visibility.remote_tombstones,
        }
    }

    pub(in crate::engine::storage_engine) fn tombstone_publication_context(
        &self,
    ) -> TombstonePublicationContext<'_> {
        TombstonePublicationContext {
            registry: &self.catalog.registry,
            tombstones: &self.visibility.tombstones,
            remote_tombstones: &self.visibility.remote_tombstones,
            tombstone_used_bytes: &self.memory.tombstone_used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn load_tombstones_index(&self) -> Result<()> {
        self.refresh_memory_usage();
        let mut reservation = self.tombstone_memory_reservation();
        let merged = self
            .tombstone_index_context()
            .read_tombstones_index(&mut reservation)?;
        self.tombstone_publication_context()
            .replace_loaded_tombstones_index(self, merged, &mut reservation)?;
        drop(reservation);
        if !self
            .tombstone_index_context()
            .tombstone_index_persist_lanes()
            .is_empty()
        {
            // Startup hydration may merge independently valid legacy lanes. Reconcile that union
            // back to every writer lane incrementally after process-lock installation.
            self.mark_tombstone_recovery_snapshot_pending();
        }
        Ok(())
    }

    fn mark_tombstone_recovery_snapshot_pending(&self) {
        let mut cursor = self
            .coordination
            .background_tombstone_recovery_snapshot_cursor
            .lock();
        *cursor = BackgroundTombstoneRecoverySnapshotCursor {
            pending: true,
            ..BackgroundTombstoneRecoverySnapshotCursor::default()
        };
    }

    fn reset_tombstone_recovery_snapshot_after_error(
        cursor: &mut BackgroundTombstoneRecoverySnapshotCursor,
    ) {
        *cursor = BackgroundTombstoneRecoverySnapshotCursor {
            pending: true,
            ..BackgroundTombstoneRecoverySnapshotCursor::default()
        };
    }

    pub(in crate::engine::storage_engine) fn bounded_tombstone_recovery_snapshot_is_pending(
        &self,
    ) -> bool {
        (self.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.runtime.maintenance_max_bytes_per_pass != u64::MAX)
            && self
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock()
                .pending
    }

    fn shrink_tombstone_recovery_snapshot_page(
        page: &mut TombstoneRecoverySnapshotPage,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let (_, ranges) = page
            .updates
            .pop_last()
            .expect("only a multi-entry recovery page is shrunk");
        page.modeled_bytes = page
            .modeled_bytes
            .saturating_sub(tombstone_recovery_snapshot_entry_bytes(&ranges));
        page.has_more = true;
        reservation.resize(page.modeled_bytes)
    }

    /// Advances one finite recovery-snapshot page while the caller holds `rollups.run_lock`.
    fn persist_tombstones_index_recovery_page_locked(
        &self,
    ) -> Result<TombstoneRecoverySnapshotProgress> {
        let mut cursor = self
            .coordination
            .background_tombstone_recovery_snapshot_cursor
            .lock();
        if !cursor.pending {
            return Ok(TombstoneRecoverySnapshotProgress::Complete);
        }

        // A page snapshot and its multi-lane transaction share the same visibility fence. A
        // concurrent delete can therefore run between pages, but never between cloning a page and
        // durably committing that exact page.
        let _visibility_guard = self.visibility_write_fence();
        if let Err(err) = self.validate_shared_object_store_writer_lock() {
            Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
            return Err(err);
        }
        // Startup planning resolves every visible coordinator before hydration, and this cursor
        // immediately rolls forward any coordinator it creates itself. Authoritative reload is
        // therefore normally a no-op probe here. The only residual whole-map reload window is an
        // unexpected committed predecessor left by another runtime tombstone writer; preserving
        // its durable delete before deriving a page is correctness-critical.
        if let Err(err) = self.recover_and_reload_tombstones_locked() {
            Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
            return Err(err);
        }
        let generation = self.tombstone_state_generation();
        if !cursor.cycle_started || cursor.observed_generation != generation {
            cursor.after_series_id = None;
            cursor.observed_generation = generation;
            cursor.cycle_started = true;
        }

        let mut reservation = self.tombstone_memory_reservation();
        let local_tombstones = self.visibility.tombstones.read();
        let remote_tombstones = self.visibility.remote_tombstones.read();
        let mut page = match collect_tombstone_recovery_snapshot_page_from_sources(
            &local_tombstones,
            &remote_tombstones,
            cursor.after_series_id,
            self.runtime.maintenance_max_items_per_pass,
            self.runtime.maintenance_max_bytes_per_pass,
            |bytes| {
                ensure_bounded_tombstone_maintenance_memory(
                    &mut reservation,
                    self.runtime.maintenance_max_bytes_per_pass,
                    bytes,
                )
            },
        ) {
            Ok(page) => page,
            Err(err) => {
                Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                return Err(err);
            }
        };
        drop(remote_tombstones);
        drop(local_tombstones);
        let index = self.tombstone_index_context();

        if page.updates.is_empty() {
            // Valid startup hydration makes the live map the union of every durable lane, and
            // ordinary removals persist an empty-range update before mutating that map. The only
            // remaining deletion-shaped reconciliation is therefore an authoritative empty map
            // against stale durable state. Preserve the old full-snapshot semantics with one
            // bounded, crash-atomic empty transaction; it carries no whole-map clone.
            let has_durable_state = match index.has_durable_tombstone_state() {
                Ok(has_durable_state) => has_durable_state,
                Err(err) => {
                    Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                    return Err(err);
                }
            };
            if has_durable_state {
                if self.runtime.maintenance_max_items_per_pass == 0 {
                    Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                    return Err(TsinkError::MaintenanceWorkItemTooLarge {
                        operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
                        limit: 0,
                        required: 1,
                    });
                }
                let empty = TombstoneMap::new();
                match index.persist_tombstones_index_snapshot_for_recovery_bounded(
                    &empty,
                    &mut reservation,
                    self.runtime.maintenance_max_bytes_per_pass,
                ) {
                    Ok(()) => {}
                    Err(error) => {
                        let definitively_clean = error.is_definitively_clean();
                        let committed = error.is_committed();
                        let err = error.into_tsink_error();
                        if definitively_clean {
                            Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                            return Err(err);
                        }
                        tracing::warn!(
                            error = %err,
                            "Unresolved empty tombstone recovery snapshot required immediate coordinator recovery"
                        );
                        let recovery = match index.recover_pending_transaction_bounded(
                            &mut reservation,
                            self.runtime.maintenance_max_bytes_per_pass,
                        ) {
                            Ok(recovery) => recovery,
                            Err(recovery_err) => {
                                Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                                return Err(TsinkError::Other(format!(
                                    "empty tombstone recovery snapshot was unresolved and coordinator recovery failed: {err}; recovery: {recovery_err}"
                                )));
                            }
                        };
                        if !committed
                            && recovery
                                != tombstone::TombstoneRecoveryOutcome::RolledForwardCommitted
                        {
                            Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                            return Err(err);
                        }
                    }
                }
            }
            *cursor = BackgroundTombstoneRecoverySnapshotCursor::default();
            return Ok(TombstoneRecoverySnapshotProgress::Complete);
        }

        loop {
            match index.transaction_staging_memory_upper_bound_bounded(
                &page.updates,
                &mut reservation,
                self.runtime.maintenance_max_bytes_per_pass,
            ) {
                Ok(_) => {}
                Err(TsinkError::MaintenanceWorkItemTooLarge { .. }) if page.updates.len() > 1 => {
                    Self::shrink_tombstone_recovery_snapshot_page(&mut page, &mut reservation)?;
                    continue;
                }
                Err(err) => {
                    Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                    return Err(err);
                }
            }

            match index.persist_tombstones_index_updates_bounded(
                &page.updates,
                &mut reservation,
                self.runtime.maintenance_max_bytes_per_pass,
            ) {
                Ok(()) => break,
                Err(error) => {
                    let definitively_clean = error.is_definitively_clean();
                    let committed = error.is_committed();
                    let err = error.into_tsink_error();
                    if definitively_clean
                        && matches!(err, TsinkError::MaintenanceWorkItemTooLarge { .. })
                        && page.updates.len() > 1
                    {
                        Self::shrink_tombstone_recovery_snapshot_page(&mut page, &mut reservation)?;
                        continue;
                    }
                    if committed {
                        // This page was derived under the still-held visibility fence, so rolling
                        // its coordinator forward cannot reveal tombstones absent from the live
                        // map. Finish the page debt before advancing; no whole-map reload is needed.
                        tracing::warn!(
                            error = %err,
                            "Committed bounded tombstone recovery page required immediate roll-forward"
                        );
                        if let Err(recovery_err) = index.recover_pending_transaction_bounded(
                            &mut reservation,
                            self.runtime.maintenance_max_bytes_per_pass,
                        ) {
                            Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                            return Err(TsinkError::Other(format!(
                                "bounded tombstone recovery page committed but roll-forward failed: {err}; recovery: {recovery_err}"
                            )));
                        }
                        break;
                    }
                    if !definitively_clean {
                        // An indeterminate decision may already have a durable Committing record.
                        // Resolve it while the page and visibility fence are still available.
                        // Reloading that partial durable page on the next wake would otherwise
                        // replace the full live map and discard tombstones not yet visited.
                        tracing::warn!(
                            error = %err,
                            "Indeterminate bounded tombstone recovery page required immediate coordinator recovery"
                        );
                        match index.recover_pending_transaction_bounded(
                            &mut reservation,
                            self.runtime.maintenance_max_bytes_per_pass,
                        ) {
                            Ok(tombstone::TombstoneRecoveryOutcome::RolledForwardCommitted) => {
                                break;
                            }
                            Ok(
                                tombstone::TombstoneRecoveryOutcome::NoTransaction
                                | tombstone::TombstoneRecoveryOutcome::RolledBackPrepared,
                            ) => {
                                Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                                return Err(err);
                            }
                            Err(recovery_err) => {
                                Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                                return Err(TsinkError::Other(format!(
                                    "bounded tombstone recovery page was indeterminate and coordinator recovery failed: {err}; recovery: {recovery_err}"
                                )));
                            }
                        }
                    }
                    Self::reset_tombstone_recovery_snapshot_after_error(&mut cursor);
                    return Err(err);
                }
            }
        }

        let last_series_id = page
            .updates
            .last_key_value()
            .map(|(&series_id, _)| series_id)
            .expect("a persisted recovery page is nonempty");
        if page.has_more {
            cursor.after_series_id = Some(last_series_id);
            cursor.observed_generation = generation;
            cursor.cycle_started = true;
            cursor.pending = true;
            Ok(TombstoneRecoverySnapshotProgress::More)
        } else {
            *cursor = BackgroundTombstoneRecoverySnapshotCursor::default();
            Ok(TombstoneRecoverySnapshotProgress::Complete)
        }
    }

    /// Runs one bounded recovery-snapshot page during a normal persisted-refresh wake.
    pub(in crate::engine::storage_engine) fn run_bounded_tombstone_recovery_snapshot_if_pending(
        &self,
    ) -> Result<()> {
        if self.runtime.maintenance_max_items_per_pass == usize::MAX
            && self.runtime.maintenance_max_bytes_per_pass == u64::MAX
        {
            return Ok(());
        }
        if !self.bounded_tombstone_recovery_snapshot_is_pending() {
            return Ok(());
        }
        let progress =
            self.with_rollup_run_lock(|| self.persist_tombstones_index_recovery_page_locked())?;
        if progress == TombstoneRecoverySnapshotProgress::More {
            self.notify_persisted_refresh_thread();
        }
        Ok(())
    }

    fn persist_tombstones_index_full_snapshot_for_recovery_locked(&self) -> Result<()> {
        let _visibility_guard = self.visibility_write_fence();
        self.validate_shared_object_store_writer_lock()?;
        self.recover_and_reload_tombstones_locked()?;
        self.refresh_memory_usage();
        let index = self.tombstone_index_context();
        let probe = index.transaction_probe_memory_upper_bound()?;
        let live_bytes = self.tombstone_read_context().memory_usage_upper_bound();
        let initial_reservation = probe.saturating_add(live_bytes).saturating_add(16 * 1024);
        let mut memory_reservation = self.tombstone_memory_reservation();
        memory_reservation.resize(initial_reservation)?;
        let snapshot = self.tombstone_read_context().snapshot();
        let transaction_staging =
            index.transaction_staging_memory_upper_bound(&snapshot, &mut memory_reservation)?;
        memory_reservation.resize(initial_reservation.max(transaction_staging))?;
        index.persist_tombstones_index_snapshot_for_recovery(&snapshot, &mut memory_reservation)
    }

    /// Reconciles tombstones while the caller holds `rollups.run_lock`.
    ///
    /// Explicit ExpertUnlimited retains the legacy one-shot snapshot. Finite configurations
    /// drain individually bounded, crash-atomic pages; normal background wakes advance one page.
    pub(in crate::engine::storage_engine) fn persist_tombstones_index_for_recovery_locked(
        &self,
    ) -> Result<()> {
        if self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly {
            // Shared lanes are read-only in this runtime. A published remote overlay must never
            // turn close into a synthetic local recovery snapshot (or an all-series union walk).
            *self
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock() = BackgroundTombstoneRecoverySnapshotCursor::default();
            return Ok(());
        }
        let index = self.tombstone_index_context();
        let live_is_empty = self.tombstone_read_context().is_empty();
        // A brand-new/never-deleted store has nothing to recover or snapshot. Avoid imposing the
        // bounded recovery scanner's fixed scratch floor during close after callers deliberately
        // tighten the live memory budget below that floor.
        if live_is_empty && !index.has_durable_tombstone_state()? {
            *self
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock() = BackgroundTombstoneRecoverySnapshotCursor::default();
            return Ok(());
        }

        if self.runtime.maintenance_max_items_per_pass == usize::MAX
            && self.runtime.maintenance_max_bytes_per_pass == u64::MAX
        {
            let result = self.persist_tombstones_index_full_snapshot_for_recovery_locked();
            if result.is_ok() {
                *self
                    .coordination
                    .background_tombstone_recovery_snapshot_cursor
                    .lock() = BackgroundTombstoneRecoverySnapshotCursor::default();
            }
            return result;
        }

        if !self
            .coordination
            .background_tombstone_recovery_snapshot_cursor
            .lock()
            .pending
        {
            self.mark_tombstone_recovery_snapshot_pending();
        }
        loop {
            match self.persist_tombstones_index_recovery_page_locked()? {
                TombstoneRecoverySnapshotProgress::Complete => return Ok(()),
                TombstoneRecoverySnapshotProgress::More => {}
            }
        }
    }

    pub(in crate::engine::storage_engine) fn timestamp_survives_tombstones(
        timestamp: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> bool {
        TombstonePublicationContext::timestamp_survives_tombstones(timestamp, tombstone_ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::config::ChunkStorageOptions;
    use super::*;
    use tempfile::TempDir;

    fn ranges(start: i64) -> Vec<tombstone::TombstoneRange> {
        vec![tombstone::TombstoneRange {
            start,
            end: start + 1,
        }]
    }

    fn map(series_ids: &[SeriesId]) -> TombstoneMap {
        series_ids
            .iter()
            .copied()
            .map(|series_id| (series_id, ranges(series_id as i64)))
            .collect()
    }

    fn immutable_snapshot(entries: TombstoneMap) -> tombstone::ImmutableTombstoneSnapshot {
        let mut shards = (0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT)
            .map(|_| TombstoneMap::new())
            .collect::<Vec<_>>();
        for (series_id, ranges) in entries {
            shards[tombstone::ImmutableTombstoneSnapshot::shard_index(series_id)]
                .insert(series_id, ranges);
        }
        tombstone::ImmutableTombstoneSnapshot::from_shards(
            shards
                .into_iter()
                .map(|entries| {
                    let bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&entries);
                    tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(entries, bytes)
                })
                .collect(),
        )
    }

    fn finite_storage(
        root: &Path,
        item_limit: usize,
        byte_limit: u64,
        two_lanes: bool,
    ) -> ChunkStorage {
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(root.join(NUMERIC_LANE_ROOT)),
            two_lanes.then(|| root.join(BLOB_LANE_ROOT)),
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                maintenance_max_items_per_pass: item_limit,
                maintenance_max_bytes_per_pass: byte_limit,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap()
    }

    fn install_live_tombstones(storage: &ChunkStorage, tombstones: TombstoneMap) {
        let mut reservation = storage.tombstone_memory_reservation();
        storage
            .tombstone_publication_context()
            .replace_loaded_tombstones_index(storage, tombstones, &mut reservation)
            .unwrap();
    }

    fn advance_page(storage: &ChunkStorage) -> Result<TombstoneRecoverySnapshotProgress> {
        storage.with_rollup_run_lock(|| storage.persist_tombstones_index_recovery_page_locked())
    }

    #[test]
    fn recovery_page_with_large_unrelated_remote_overlay_clones_only_selected_series() {
        let local = TombstoneMap::from([(7, vec![tombstone::TombstoneRange { start: 0, end: 1 }])]);
        let mut remote = (1_000..11_000)
            .map(|series_id| (series_id, ranges(series_id as i64)))
            .collect::<TombstoneMap>();
        remote.insert(7, vec![tombstone::TombstoneRange { start: 2, end: 3 }]);
        let remote = immutable_snapshot(remote);
        let mut admission_calls = 0usize;
        let page = collect_tombstone_recovery_snapshot_page_from_sources(
            &local,
            &remote,
            None,
            1,
            u64::MAX,
            |_| {
                admission_calls = admission_calls.saturating_add(1);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(admission_calls, 1);
        assert_eq!(
            page.updates,
            TombstoneMap::from([(
                7,
                vec![
                    tombstone::TombstoneRange { start: 0, end: 1 },
                    tombstone::TombstoneRange { start: 2, end: 3 },
                ],
            )]),
        );
        assert!(page.has_more);
    }

    #[test]
    fn recovery_page_item_limit_admits_exact_n_and_defers_n_plus_one() {
        let tombstones = map(&[1, 2, 3]);
        let enough_bytes = u64::MAX;

        let one = collect_tombstone_recovery_snapshot_page(
            &tombstones,
            None,
            1,
            enough_bytes,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(one.updates.keys().copied().collect::<Vec<_>>(), vec![1]);
        assert!(one.has_more);

        let exact = collect_tombstone_recovery_snapshot_page(
            &tombstones,
            None,
            2,
            enough_bytes,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(
            exact.updates.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(exact.has_more);

        let terminal = collect_tombstone_recovery_snapshot_page(
            &tombstones,
            None,
            3,
            enough_bytes,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(terminal.updates, tombstones);
        assert!(!terminal.has_more);
    }

    #[test]
    fn recovery_page_byte_limit_rejects_n_minus_one_and_admits_exact_n() {
        let tombstones = map(&[7, 8]);
        let entry_bytes = tombstone_recovery_snapshot_entry_bytes(tombstones.get(&7).unwrap());
        let one_required = TOMBSTONE_RECOVERY_SNAPSHOT_PAGE_BASE_BYTES + entry_bytes;

        let err = collect_tombstone_recovery_snapshot_page(
            &tombstones,
            None,
            usize::MAX,
            (one_required - 1) as u64,
            |_| Ok(()),
        )
        .err()
        .expect("one byte below the modeled item must reject");
        assert!(matches!(
            err,
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: TOMBSTONE_RECOVERY_SNAPSHOT_OPERATION,
                limit,
                required,
            } if limit == (one_required - 1) as u64 && required == one_required as u64
        ));

        let mut admitted = 0usize;
        let exact = collect_tombstone_recovery_snapshot_page(
            &tombstones,
            None,
            usize::MAX,
            one_required as u64,
            |bytes| {
                admitted = bytes;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(admitted, one_required);
        assert_eq!(exact.updates.keys().copied().collect::<Vec<_>>(), vec![7]);
        assert!(exact.has_more);

        let two_required = one_required + entry_bytes;
        let both = collect_tombstone_recovery_snapshot_page(
            &tombstones,
            None,
            usize::MAX,
            two_required as u64,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(both.updates, tombstones);
        assert!(!both.has_more);
    }

    #[test]
    fn finite_recovery_snapshot_continues_across_wakes_and_publishes_every_lane() {
        let temp = TempDir::new().unwrap();
        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, true);
        let expected = map(&[1, 2, 3]);
        install_live_tombstones(&storage, expected.clone());
        storage.mark_tombstone_recovery_snapshot_pending();

        storage
            .run_bounded_tombstone_recovery_snapshot_if_pending()
            .unwrap();
        assert_eq!(
            storage
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock()
                .after_series_id,
            Some(1)
        );
        storage
            .run_bounded_tombstone_recovery_snapshot_if_pending()
            .unwrap();
        assert_eq!(
            storage
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock()
                .after_series_id,
            Some(2)
        );
        storage
            .run_bounded_tombstone_recovery_snapshot_if_pending()
            .unwrap();

        for lane in [NUMERIC_LANE_ROOT, BLOB_LANE_ROOT] {
            assert_eq!(
                tombstone::load_tombstones(&temp.path().join(lane).join(TOMBSTONES_FILE_NAME))
                    .unwrap(),
                expected
            );
        }
        assert!(!storage.bounded_tombstone_recovery_snapshot_is_pending());
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn unrelated_visibility_churn_keeps_cursor_but_tombstone_mutation_restarts_it() {
        let temp = TempDir::new().unwrap();
        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, false);
        install_live_tombstones(&storage, map(&[10, 20, 30]));
        storage.mark_tombstone_recovery_snapshot_pending();

        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );
        storage.bump_visibility_state_generation();
        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );
        assert_eq!(
            storage
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock()
                .after_series_id,
            Some(20),
            "catalog-only visibility churn must not restart tombstone paging"
        );

        let mut changed = storage.tombstone_read_context().snapshot();
        changed.insert(5, ranges(5));
        install_live_tombstones(&storage, changed);
        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );
        assert_eq!(
            storage
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock()
                .after_series_id,
            Some(5),
            "a tombstone-map mutation must restart at the ordered beginning"
        );
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn clean_page_error_resets_for_retry_and_releases_reservation() {
        let temp = TempDir::new().unwrap();
        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, false);
        install_live_tombstones(&storage, map(&[1, 2]));
        storage.mark_tombstone_recovery_snapshot_pending();

        let hook = tombstone::fail_tombstone_transaction_once(
            tombstone::TombstoneTransactionTestPoint::BeforeCommitDecision,
            "injected bounded recovery page failure",
        );
        assert!(advance_page(&storage).is_err());
        drop(hook);
        assert_eq!(
            *storage
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock(),
            BackgroundTombstoneRecoverySnapshotCursor {
                pending: true,
                ..BackgroundTombstoneRecoverySnapshotCursor::default()
            }
        );
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );

        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );
        assert_eq!(
            tombstone::load_tombstones(
                &temp
                    .path()
                    .join(NUMERIC_LANE_ROOT)
                    .join(TOMBSTONES_FILE_NAME)
            )
            .unwrap(),
            map(&[1])
        );
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn committed_page_error_rolls_forward_before_cursor_advance() {
        let temp = TempDir::new().unwrap();
        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, true);
        install_live_tombstones(&storage, map(&[1, 2]));
        storage.mark_tombstone_recovery_snapshot_pending();

        let hook = tombstone::fail_tombstone_transaction_once(
            tombstone::TombstoneTransactionTestPoint::AfterCommitDecision,
            "injected committed bounded recovery page interruption",
        );
        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );
        drop(hook);

        for lane in [NUMERIC_LANE_ROOT, BLOB_LANE_ROOT] {
            assert_eq!(
                tombstone::load_tombstones(&temp.path().join(lane).join(TOMBSTONES_FILE_NAME))
                    .unwrap(),
                map(&[1])
            );
        }
        assert!(!temp
            .path()
            .join(tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
            .join(tombstone::TOMBSTONE_TRANSACTION_FILE_NAME)
            .exists());
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn indeterminate_committing_page_is_resolved_without_reloading_partial_durable_state() {
        let temp = TempDir::new().unwrap();
        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, true);
        let live = map(&[1, 2]);
        install_live_tombstones(&storage, live.clone());
        storage.mark_tombstone_recovery_snapshot_pending();

        let hook = tombstone::fail_tombstone_transaction_once(
            tombstone::TombstoneTransactionTestPoint::AmbiguousCommitDecision,
            "injected indeterminate bounded recovery page decision",
        );
        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );
        drop(hook);

        assert_eq!(
            storage.tombstone_read_context().snapshot(),
            live,
            "resolving the first durable page must not replace the still-authoritative live map"
        );
        assert_eq!(
            storage
                .coordination
                .background_tombstone_recovery_snapshot_cursor
                .lock()
                .after_series_id,
            Some(1)
        );
        for lane in [NUMERIC_LANE_ROOT, BLOB_LANE_ROOT] {
            assert_eq!(
                tombstone::load_tombstones(&temp.path().join(lane).join(TOMBSTONES_FILE_NAME))
                    .unwrap(),
                map(&[1])
            );
        }
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn finite_close_path_drains_an_existing_multi_page_cursor() {
        let temp = TempDir::new().unwrap();
        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, true);
        let expected = map(&[1, 2, 3]);
        install_live_tombstones(&storage, expected.clone());
        storage.mark_tombstone_recovery_snapshot_pending();
        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::More
        );

        storage
            .with_rollup_run_lock(|| storage.persist_tombstones_index_for_recovery_locked())
            .unwrap();
        for lane in [NUMERIC_LANE_ROOT, BLOB_LANE_ROOT] {
            assert_eq!(
                tombstone::load_tombstones(&temp.path().join(lane).join(TOMBSTONES_FILE_NAME))
                    .unwrap(),
                expected
            );
        }
        assert!(!storage.bounded_tombstone_recovery_snapshot_is_pending());
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn authoritative_empty_map_removes_stale_durable_tombstones_boundedly() {
        let temp = TempDir::new().unwrap();
        let lane_path = temp.path().join(NUMERIC_LANE_ROOT);
        let manifest_path = lane_path.join(TOMBSTONES_FILE_NAME);
        tombstone::persist_tombstones(&manifest_path, &map(&[7, 9])).unwrap();

        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, false);
        storage.load_tombstones_index().unwrap();
        install_live_tombstones(&storage, TombstoneMap::new());
        storage.mark_tombstone_recovery_snapshot_pending();

        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::Complete
        );
        assert_eq!(
            tombstone::load_tombstones(&manifest_path).unwrap(),
            TombstoneMap::new()
        );
        assert!(!manifest_path.exists());
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn indeterminate_empty_snapshot_is_resolved_before_reporting_completion() {
        let temp = TempDir::new().unwrap();
        let lane_path = temp.path().join(NUMERIC_LANE_ROOT);
        let manifest_path = lane_path.join(TOMBSTONES_FILE_NAME);
        tombstone::persist_tombstones(&manifest_path, &map(&[7])).unwrap();

        let storage = finite_storage(temp.path(), 1, 256 * 1024 * 1024, false);
        storage.load_tombstones_index().unwrap();
        install_live_tombstones(&storage, TombstoneMap::new());
        storage.mark_tombstone_recovery_snapshot_pending();

        let hook = tombstone::fail_tombstone_transaction_once(
            tombstone::TombstoneTransactionTestPoint::AmbiguousCommitDecision,
            "injected indeterminate empty recovery snapshot decision",
        );
        assert_eq!(
            advance_page(&storage).unwrap(),
            TombstoneRecoverySnapshotProgress::Complete
        );
        drop(hook);

        assert_eq!(
            tombstone::load_tombstones(&manifest_path).unwrap(),
            TombstoneMap::new()
        );
        assert!(!manifest_path.exists());
        assert!(!storage.bounded_tombstone_recovery_snapshot_is_pending());
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
    }
}
