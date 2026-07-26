use std::mem;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::Ordering;

use crate::engine::tombstone::{
    self, RemoteTombstoneManifestFingerprint, RemoteTombstoneManifestKind, TombstoneLane,
    TombstoneMap,
};

use super::bounded_remote::RemoteCatalogPassBudget;
use super::*;

const REMOTE_TOMBSTONE_MANIFEST_OPERATION: &str = "finite remote tombstone manifest validation";
const REMOTE_TOMBSTONE_SHARD_OPERATION: &str = "finite remote tombstone shard staging";
const REMOTE_TOMBSTONE_REVALIDATION_OPERATION: &str =
    "finite remote tombstone manifest revalidation";
const REMOTE_TOMBSTONE_CURSOR_INITIALIZATION_OPERATION: &str =
    "finite remote tombstone cursor initialization";
const REMOTE_TOMBSTONE_CANDIDATE_SETUP_OPERATION: &str =
    "finite remote tombstone immutable candidate setup";
const REMOTE_TOMBSTONE_CHANGE_CHECK_OPERATION: &str =
    "finite remote tombstone immutable shard change check";
const REMOTE_TOMBSTONE_BASE_ENTRY_OPERATION: &str =
    "finite remote tombstone immutable base shard copy";
const REMOTE_TOMBSTONE_MERGE_ENTRY_OPERATION: &str =
    "finite remote tombstone immutable shard merge";
const REMOTE_TOMBSTONE_PUBLICATION_OPERATION: &str = "finite remote tombstone atomic publication";
const REMOTE_TOMBSTONE_CURSOR_FIXED_BYTES: u64 = 16 * 1024;
const REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES: u64 = 32 * 1024;
const REMOTE_TOMBSTONE_PUBLICATION_FIXED_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
enum PinnedRemoteTombstoneManifestKind {
    Missing,
    Sharded(Vec<Option<String>>),
}

#[derive(Debug)]
struct PinnedRemoteTombstoneManifest {
    fingerprint: RemoteTombstoneManifestFingerprint,
    kind: PinnedRemoteTombstoneManifestKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedRemoteTombstoneRefreshPhase {
    Initializing,
    Manifests {
        lane_index: usize,
    },
    Shards {
        lane_index: usize,
        shard_index: usize,
    },
    Revalidating {
        lane_index: usize,
    },
    CandidateSetup,
    CheckingShardFragments {
        shard_index: usize,
        fragment_index: usize,
        after_series_id: Option<SeriesId>,
    },
    CopyingBaseShard {
        shard_index: usize,
        after_series_id: Option<SeriesId>,
    },
    MergingShardFragments {
        shard_index: usize,
        fragment_index: usize,
    },
    Publishing,
}

#[derive(Debug)]
pub(super) enum BoundedRemoteTombstoneRefreshOutcome {
    Progressed,
    Deferred,
    Restart,
    Published { visibility_generation: u64 },
}

enum TerminalManifestRevalidation {
    Deferred,
    Changed,
    Validated,
}

pub(super) struct BoundedRemoteTombstoneRefreshCycle {
    lanes: Vec<TombstoneLane>,
    manifests: Vec<PinnedRemoteTombstoneManifest>,
    fragments_by_shard: Vec<Vec<TombstoneMap>>,
    prior_remote_snapshot: Option<Arc<tombstone::ImmutableTombstoneSnapshot>>,
    candidate_shards: Vec<Option<Arc<tombstone::ImmutableTombstoneShard>>>,
    current_candidate_shard: TombstoneMap,
    current_candidate_shard_memory_bytes: usize,
    candidate_changed: bool,
    phase: BoundedRemoteTombstoneRefreshPhase,
    initialization_precharged: bool,
    accounted_retained_bytes: u64,
    initial_retained_bytes: u64,
    #[cfg(test)]
    release_after_drop_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl BoundedRemoteTombstoneRefreshCycle {
    fn modeled_initial_retained_bytes(lanes: &[TombstoneLane]) -> u64 {
        let lane_bytes = lanes.iter().fold(0u64, |total, lane| {
            total
                .saturating_add(
                    u64::try_from(lane.namespace_root.as_os_str().as_encoded_bytes().len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(2),
                )
                .saturating_add(
                    u64::try_from(lane.manifest_path.as_os_str().as_encoded_bytes().len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(2),
                )
                .saturating_add(u64::try_from(mem::size_of::<TombstoneLane>()).unwrap_or(u64::MAX))
        });
        REMOTE_TOMBSTONE_CURSOR_FIXED_BYTES.saturating_add(lane_bytes)
    }

    fn from_admitted_lanes(
        storage: &ChunkStorage,
        lanes: Vec<TombstoneLane>,
        mut reservation: super::super::TombstoneMemoryReservation<'_>,
        initialization_precharged: bool,
    ) -> Result<Self> {
        let initial_retained_bytes = Self::modeled_initial_retained_bytes(&lanes);
        let initial_usize = usize::try_from(initial_retained_bytes).map_err(|_| {
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_CURSOR_INITIALIZATION_OPERATION,
                limit: usize::MAX as u64,
                required: initial_retained_bytes,
            }
        })?;
        reservation.ensure(initial_usize)?;
        let mut cycle = Self {
            lanes,
            manifests: Vec::new(),
            fragments_by_shard: (0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT)
                .map(|_| Vec::new())
                .collect(),
            prior_remote_snapshot: None,
            candidate_shards: Vec::new(),
            current_candidate_shard: TombstoneMap::new(),
            current_candidate_shard_memory_bytes: 0,
            candidate_changed: false,
            phase: BoundedRemoteTombstoneRefreshPhase::Initializing,
            initialization_precharged,
            accounted_retained_bytes: 0,
            initial_retained_bytes,
            #[cfg(test)]
            release_after_drop_hook: None,
        };
        cycle.retain_operation_bytes(storage, initial_retained_bytes, &mut reservation)?;
        Ok(cycle)
    }

    /// Production constructor: maintenance work and global memory are both admitted before the
    /// fixed-fanout lane vector or any owned lane path is materialized.
    pub(super) fn new_from_storage(
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<Option<Self>> {
        let context = storage.tombstone_index_context();
        let construction_bytes = context.tombstone_lane_construction_upper_bound(true);
        let construction_work = u64::try_from(construction_bytes).unwrap_or(u64::MAX);
        if !budget.charge_for(
            REMOTE_TOMBSTONE_CURSOR_INITIALIZATION_OPERATION,
            1,
            construction_work,
        )? {
            return Ok(None);
        }
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(construction_bytes)?;
        let lanes = context.remote_tombstone_refresh_lanes()?;
        Self::from_admitted_lanes(storage, lanes, reservation, true).map(Some)
    }

    #[cfg(test)]
    pub(super) fn new(storage: &ChunkStorage, lanes: Vec<TombstoneLane>) -> Result<Self> {
        let reservation = storage.tombstone_memory_reservation();
        Self::from_admitted_lanes(storage, lanes, reservation, false)
    }

    fn manifest_work_bytes(manifest_len: Option<usize>) -> u64 {
        manifest_len.map_or(0, |manifest_len| {
            u64::try_from(manifest_len)
                .unwrap_or(u64::MAX)
                .saturating_mul(64)
                .saturating_add(4096)
        })
    }

    fn shard_work_bytes(shard_len: usize) -> u64 {
        // The strict shard preflight bounds both entry and range counts by encoded bytes.
        // A valid zero-range entry occupies only sixteen encoded bytes but retains the modeled
        // 512-byte B-tree allocation. Sixty-four retained bytes per encoded byte therefore cover
        // the encoded buffer, least-dense decoded map, range payloads, and allocator growth.
        u64::try_from(shard_len)
            .unwrap_or(u64::MAX)
            .saturating_mul(64)
            .saturating_add(16 * 1024)
    }

    fn revalidation_work_bytes(manifest_len: Option<usize>) -> u64 {
        manifest_len.map_or(0, |manifest_len| {
            u64::try_from(manifest_len)
                .unwrap_or(u64::MAX)
                .saturating_add(4096)
        })
    }

    /// Revalidates the complete fixed-fanout manifest set in the same visibility-fence hold as
    /// the terminal pointer decision. Earlier per-lane validation protects long candidate builds;
    /// this final bounded sweep closes the multi-wake gap immediately before publication.
    fn terminal_revalidate_manifests(
        &self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        reservation: &mut super::super::TombstoneMemoryReservation<'_>,
        simultaneously_live_bytes: usize,
        terminal_items: usize,
        terminal_work_bytes: u64,
        terminal_operation: &'static str,
    ) -> Result<TerminalManifestRevalidation> {
        // Admit the fixed-size metadata probe and its length vector before the first stat. The
        // observed current lengths, rather than stale pinned lengths, then determine the exact
        // cumulative read/decode charge.
        let probe_work_bytes = u64::try_from(self.lanes.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(4096);
        if !budget.charge_for(
            terminal_operation,
            terminal_items,
            terminal_work_bytes.saturating_add(probe_work_bytes),
        )? {
            return Ok(TerminalManifestRevalidation::Deferred);
        }
        let lengths_bytes = self
            .lanes
            .len()
            .saturating_mul(mem::size_of::<Option<usize>>())
            .saturating_add(4096);
        reservation.ensure(
            simultaneously_live_bytes
                .saturating_add(lengths_bytes)
                .saturating_add(usize::try_from(probe_work_bytes).unwrap_or(usize::MAX)),
        )?;
        let mut observed_lengths = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            observed_lengths.push(tombstone::remote_tombstone_manifest_file_len(lane)?);
        }
        let read_work_bytes = observed_lengths.iter().fold(0u64, |total, manifest_len| {
            total.saturating_add(Self::revalidation_work_bytes(*manifest_len).max(4096))
        });
        let total_work_bytes = terminal_work_bytes
            .saturating_add(probe_work_bytes)
            .saturating_add(read_work_bytes);
        if total_work_bytes > budget.byte_limit() {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: terminal_operation,
                limit: budget.byte_limit(),
                required: total_work_bytes,
            });
        }
        if !budget.charge_for(terminal_operation, 0, read_work_bytes)? {
            return Ok(TerminalManifestRevalidation::Deferred);
        }
        reservation.ensure(
            simultaneously_live_bytes
                .saturating_add(lengths_bytes)
                .saturating_add(usize::try_from(read_work_bytes).unwrap_or(usize::MAX)),
        )?;
        for (lane_index, (lane, observed_len)) in
            self.lanes.iter().zip(observed_lengths).enumerate()
        {
            let expected = self
                .manifests
                .get(lane_index)
                .ok_or_else(|| {
                    TsinkError::DataCorruption(
                        "finite remote terminal revalidation lost its pinned manifest".to_string(),
                    )
                })?
                .fingerprint;
            if tombstone::remote_tombstone_manifest_file_len(lane)? != observed_len {
                return Ok(TerminalManifestRevalidation::Changed);
            }
            let observed = match tombstone::revalidate_remote_tombstone_manifest(lane, observed_len)
            {
                Ok(observed) => observed,
                Err(_err)
                    if tombstone::remote_tombstone_manifest_file_len(lane)? != observed_len =>
                {
                    return Ok(TerminalManifestRevalidation::Changed)
                }
                Err(err) => return Err(err),
            };
            if observed != expected {
                return Ok(TerminalManifestRevalidation::Changed);
            }
        }
        // `storage` is intentionally part of this helper's contract: callers must invoke it only
        // while holding this storage's visibility fence.
        debug_assert_eq!(
            storage.runtime.runtime_mode,
            StorageRuntimeMode::ComputeOnly
        );
        Ok(TerminalManifestRevalidation::Validated)
    }

    #[cfg(test)]
    fn terminal_publication_work_bytes_for_test(&self) -> u64 {
        let probe = u64::try_from(self.lanes.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(4096);
        self.lanes.iter().fold(
            REMOTE_TOMBSTONE_PUBLICATION_FIXED_BYTES.saturating_add(probe),
            |total, lane| {
                let len = tombstone::remote_tombstone_manifest_file_len(lane)
                    .expect("test terminal manifest metadata must be readable");
                total.saturating_add(Self::revalidation_work_bytes(len).max(4096))
            },
        )
    }

    fn base_entry_work_bytes(ranges: &[tombstone::TombstoneRange]) -> u64 {
        u64::try_from(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(
                u64::try_from(ranges.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(
                        u64::try_from(mem::size_of::<tombstone::TombstoneRange>())
                            .unwrap_or(u64::MAX),
                    )
                    .saturating_mul(2),
            )
            .saturating_add(4096)
    }

    fn merge_entry_work_bytes(
        current: Option<&[tombstone::TombstoneRange]>,
        additional: &[tombstone::TombstoneRange],
    ) -> u64 {
        // The old destination and decoded source coexist with one linear owned merge buffer.
        u64::try_from(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(
                u64::try_from(
                    current
                        .map_or(0, |ranges| ranges.len())
                        .saturating_add(additional.len()),
                )
                .unwrap_or(u64::MAX)
                .saturating_mul(
                    u64::try_from(mem::size_of::<tombstone::TombstoneRange>()).unwrap_or(u64::MAX),
                )
                .saturating_mul(2),
            )
            .saturating_add(4096)
    }

    fn change_check_work_bytes(additional: &[tombstone::TombstoneRange]) -> u64 {
        u64::try_from(additional.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(
                u64::try_from(mem::size_of::<tombstone::TombstoneRange>()).unwrap_or(u64::MAX),
            )
            .saturating_add(4096)
    }

    fn retain_operation_bytes(
        &mut self,
        storage: &ChunkStorage,
        additional: u64,
        reservation: &mut super::super::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        if additional == 0 {
            return Ok(());
        }
        let additional_usize =
            usize::try_from(additional).map_err(|_| TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_SHARD_OPERATION,
                limit: usize::MAX as u64,
                required: additional,
            })?;
        reservation.ensure(additional_usize)?;
        // The short-lived reservation has already admitted these bytes. Transfer ownership to
        // the resumable cursor by adding an equal persistent charge immediately before the
        // reservation drops.
        storage
            .memory
            .tombstone_staged_bytes
            .fetch_add(additional, Ordering::AcqRel);
        self.accounted_retained_bytes = self.accounted_retained_bytes.saturating_add(additional);
        Ok(())
    }

    pub(super) fn release_retained_bytes(&mut self, storage: &ChunkStorage) {
        // Drop every cursor-owned heap allocation while its persistent staging charge is still
        // visible. A concurrent reservation must never observe the bytes as free while decoded
        // fragments, path buffers, or candidate maps remain reachable from this cursor.
        drop(mem::take(&mut self.lanes));
        drop(mem::take(&mut self.manifests));
        drop(mem::take(&mut self.fragments_by_shard));
        drop(self.prior_remote_snapshot.take());
        drop(mem::take(&mut self.candidate_shards));
        drop(mem::take(&mut self.current_candidate_shard));
        self.current_candidate_shard_memory_bytes = 0;
        #[cfg(test)]
        if let Some(hook) = self.release_after_drop_hook.as_ref() {
            hook();
        }
        let retained = mem::take(&mut self.accounted_retained_bytes);
        if retained == 0 {
            return;
        }
        let _ = storage.memory.tombstone_staged_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(retained)),
        );
    }

    pub(super) fn has_no_retained_bytes(&self) -> bool {
        self.accounted_retained_bytes == 0
    }

    #[cfg(test)]
    fn merge_fragment(candidate: &mut TombstoneMap, fragment: &TombstoneMap) -> usize {
        let mut consumed = 0usize;
        for (&series_id, ranges) in fragment {
            let candidate_ranges = candidate.entry(series_id).or_default();
            consumed = consumed.saturating_add(tombstone::merge_normalized_tombstone_ranges(
                candidate_ranges,
                ranges,
            ));
        }
        consumed
    }

    fn advance_initialization(
        &mut self,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        if !self.initialization_precharged
            && !budget.charge_for(
                REMOTE_TOMBSTONE_CURSOR_INITIALIZATION_OPERATION,
                1,
                self.initial_retained_bytes,
            )?
        {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        self.initialization_precharged = false;
        self.phase = BoundedRemoteTombstoneRefreshPhase::Manifests { lane_index: 0 };
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn shard_failure_after_manifest_revalidation(
        &self,
        storage: &ChunkStorage,
        lane_index: usize,
        lane: &TombstoneLane,
        budget: &mut RemoteCatalogPassBudget,
        shard_error: TsinkError,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        let expected = self
            .manifests
            .get(lane_index)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "finite remote tombstone shard recovery lost its pinned manifest".to_string(),
                )
            })?
            .fingerprint;
        let manifest_len = tombstone::remote_tombstone_manifest_file_len(lane)?;
        let work_bytes = Self::revalidation_work_bytes(manifest_len);
        if !budget.charge_for(REMOTE_TOMBSTONE_REVALIDATION_OPERATION, 1, work_bytes)? {
            // The parent discards every tombstone subcycle whose advance returns an error, so
            // propagating here still guarantees a fresh manifest on the next eligible wake.
            return Err(shard_error);
        }
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(usize::try_from(work_bytes).map_err(|_| {
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_REVALIDATION_OPERATION,
                limit: usize::MAX as u64,
                required: work_bytes,
            }
        })?)?;
        let observed = tombstone::revalidate_remote_tombstone_manifest(lane, manifest_len)?;
        if observed != expected {
            Ok(BoundedRemoteTombstoneRefreshOutcome::Restart)
        } else {
            Err(shard_error)
        }
    }

    fn advance_manifest(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        lane_index: usize,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        let Some(lane) = self.lanes.get(lane_index).cloned() else {
            self.phase = BoundedRemoteTombstoneRefreshPhase::Shards {
                lane_index: 0,
                shard_index: 0,
            };
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
        };
        let manifest_len = tombstone::remote_tombstone_manifest_file_len(&lane)?;
        let manifest_bytes = Self::manifest_work_bytes(manifest_len);
        let work_bytes = manifest_bytes;
        if work_bytes != 0
            && !budget.charge_for(REMOTE_TOMBSTONE_MANIFEST_OPERATION, 1, work_bytes)?
        {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        let work_bytes_usize =
            usize::try_from(work_bytes).map_err(|_| TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_MANIFEST_OPERATION,
                limit: usize::MAX as u64,
                required: work_bytes,
            })?;
        let mut reservation = storage.tombstone_memory_reservation();
        if work_bytes_usize != 0 {
            reservation.ensure(work_bytes_usize)?;
        }
        let snapshot = tombstone::load_remote_tombstone_manifest_snapshot(&lane, manifest_len)?;
        let kind = match snapshot.kind {
            RemoteTombstoneManifestKind::Missing => PinnedRemoteTombstoneManifestKind::Missing,
            RemoteTombstoneManifestKind::Legacy => {
                return Err(TsinkError::UnsupportedOperation {
                    operation: REMOTE_TOMBSTONE_MANIFEST_OPERATION,
                    reason: "finite remote refresh requires the sharded tombstone format; migrate the legacy monolithic tombstone file with ExpertUnlimited"
                        .to_string(),
                })
            }
            RemoteTombstoneManifestKind::Sharded(shards) => {
                PinnedRemoteTombstoneManifestKind::Sharded(shards)
            }
        };
        self.manifests.push(PinnedRemoteTombstoneManifest {
            fingerprint: snapshot.fingerprint,
            kind,
        });
        self.phase = BoundedRemoteTombstoneRefreshPhase::Manifests {
            lane_index: lane_index.saturating_add(1),
        };
        if work_bytes != 0 {
            self.retain_operation_bytes(storage, work_bytes, &mut reservation)?;
        }
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn next_referenced_shard(
        &self,
        mut lane_index: usize,
        mut shard_index: usize,
    ) -> Option<(usize, usize, String)> {
        while lane_index < self.manifests.len() {
            let manifest = &self.manifests[lane_index];
            if let PinnedRemoteTombstoneManifestKind::Sharded(shards) = &manifest.kind {
                while shard_index < shards.len() {
                    let current_index = shard_index;
                    shard_index = shard_index.saturating_add(1);
                    if let Some(file_name) = shards[current_index].as_ref() {
                        return Some((lane_index, current_index, file_name.clone()));
                    }
                }
            }
            lane_index = lane_index.saturating_add(1);
            shard_index = 0;
        }
        None
    }

    fn advance_shard(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        lane_index: usize,
        shard_index: usize,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        let Some((lane_index, shard_index, file_name)) =
            self.next_referenced_shard(lane_index, shard_index)
        else {
            self.phase = BoundedRemoteTombstoneRefreshPhase::Revalidating { lane_index: 0 };
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
        };
        let lane = self.lanes.get(lane_index).cloned().ok_or_else(|| {
            TsinkError::DataCorruption(
                "finite remote tombstone shard cursor lost its lane".to_string(),
            )
        })?;
        let shard_len =
            match tombstone::remote_tombstone_shard_file_len(&lane, shard_index, &file_name) {
                Ok(shard_len) => shard_len,
                Err(err) => {
                    return self.shard_failure_after_manifest_revalidation(
                        storage, lane_index, &lane, budget, err,
                    )
                }
            };
        let work_bytes = Self::shard_work_bytes(shard_len);
        if !budget.charge_for(REMOTE_TOMBSTONE_SHARD_OPERATION, 1, work_bytes)? {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        let work_bytes_usize =
            usize::try_from(work_bytes).map_err(|_| TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_SHARD_OPERATION,
                limit: usize::MAX as u64,
                required: work_bytes,
            })?;
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(work_bytes_usize)?;
        let fragment = match tombstone::load_remote_tombstone_shard_with_memory_admission(
            &lane,
            shard_index,
            &file_name,
            shard_len,
            |peak| reservation.ensure(peak),
        ) {
            Ok(fragment) => fragment,
            Err(err) => {
                return self.shard_failure_after_manifest_revalidation(
                    storage, lane_index, &lane, budget, err,
                )
            }
        };
        self.fragments_by_shard[shard_index].push(fragment);
        self.phase = BoundedRemoteTombstoneRefreshPhase::Shards {
            lane_index,
            shard_index: shard_index.saturating_add(1),
        };
        self.retain_operation_bytes(storage, work_bytes, &mut reservation)?;
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn advance_revalidation(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        lane_index: usize,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        let Some(lane) = self.lanes.get(lane_index) else {
            self.phase = BoundedRemoteTombstoneRefreshPhase::CandidateSetup;
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
        };
        let manifest_len = tombstone::remote_tombstone_manifest_file_len(lane)?;
        let work_bytes = Self::revalidation_work_bytes(manifest_len);
        let expected = self
            .manifests
            .get(lane_index)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "finite remote tombstone revalidation lost its pinned manifest".to_string(),
                )
            })?
            .fingerprint;
        let unchanged_missing = manifest_len.is_none() && !expected.exists;
        if !unchanged_missing
            && !budget.charge_for(REMOTE_TOMBSTONE_REVALIDATION_OPERATION, 1, work_bytes)?
        {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(usize::try_from(work_bytes).map_err(|_| {
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_REVALIDATION_OPERATION,
                limit: usize::MAX as u64,
                required: work_bytes,
            }
        })?)?;
        let observed = tombstone::revalidate_remote_tombstone_manifest(lane, manifest_len)?;
        if observed != expected {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart);
        }
        self.phase = BoundedRemoteTombstoneRefreshPhase::Revalidating {
            lane_index: lane_index.saturating_add(1),
        };
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn candidate_is_empty(&self) -> bool {
        self.fragments_by_shard
            .iter()
            .all(|fragments| fragments.iter().all(TombstoneMap::is_empty))
    }

    fn setup_candidate(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        expected_visibility_generation: u64,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        if self.candidate_is_empty() {
            let mut reservation = storage.tombstone_memory_reservation();
            let _visibility_guard = storage.visibility_write_fence();
            if storage.visibility_state_generation() != expected_visibility_generation {
                return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart);
            }
            match self.terminal_revalidate_manifests(
                storage,
                budget,
                &mut reservation,
                0,
                1,
                0,
                REMOTE_TOMBSTONE_REVALIDATION_OPERATION,
            )? {
                TerminalManifestRevalidation::Deferred => {
                    return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred)
                }
                TerminalManifestRevalidation::Changed => {
                    return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart)
                }
                TerminalManifestRevalidation::Validated => {}
            }
            self.release_retained_bytes(storage);
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Published {
                visibility_generation: expected_visibility_generation,
            });
        }
        if !budget.charge_for(
            REMOTE_TOMBSTONE_CANDIDATE_SETUP_OPERATION,
            1,
            REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES,
        )? {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        let setup_bytes =
            usize::try_from(REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES).map_err(|_| {
                TsinkError::MaintenanceWorkItemTooLarge {
                    operation: REMOTE_TOMBSTONE_CANDIDATE_SETUP_OPERATION,
                    limit: usize::MAX as u64,
                    required: REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES,
                }
            })?;
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(setup_bytes)?;
        if storage.visibility_state_generation() != expected_visibility_generation {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart);
        }
        self.prior_remote_snapshot = Some(storage.tombstone_read_context().remote_snapshot());
        self.candidate_shards = (0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT)
            .map(|_| None)
            .collect();
        self.phase = BoundedRemoteTombstoneRefreshPhase::CheckingShardFragments {
            shard_index: 0,
            fragment_index: 0,
            after_series_id: None,
        };
        self.retain_operation_bytes(
            storage,
            REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES,
            &mut reservation,
        )?;
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn advance_candidate_change_check(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        mut shard_index: usize,
        mut fragment_index: usize,
        mut after_series_id: Option<SeriesId>,
        expected_visibility_generation: u64,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        loop {
            if shard_index == tombstone::LIVE_TOMBSTONE_SHARD_COUNT {
                if !self.candidate_changed {
                    let mut reservation = storage.tombstone_memory_reservation();
                    let _visibility_guard = storage.visibility_write_fence();
                    if storage.visibility_state_generation() != expected_visibility_generation {
                        return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart);
                    }
                    match self.terminal_revalidate_manifests(
                        storage,
                        budget,
                        &mut reservation,
                        0,
                        1,
                        0,
                        REMOTE_TOMBSTONE_REVALIDATION_OPERATION,
                    )? {
                        TerminalManifestRevalidation::Deferred => {
                            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred)
                        }
                        TerminalManifestRevalidation::Changed => {
                            return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart)
                        }
                        TerminalManifestRevalidation::Validated => {}
                    }
                    drop(self.prior_remote_snapshot.take());
                    self.release_retained_bytes(storage);
                    return Ok(BoundedRemoteTombstoneRefreshOutcome::Published {
                        visibility_generation: expected_visibility_generation,
                    });
                }
                self.phase = BoundedRemoteTombstoneRefreshPhase::Publishing;
                return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
            }
            let prior = self.prior_remote_snapshot.as_ref().ok_or_else(|| {
                TsinkError::DataCorruption(
                    "finite remote tombstone change check lost its pinned live snapshot"
                        .to_string(),
                )
            })?;
            if fragment_index == self.fragments_by_shard[shard_index].len() {
                self.candidate_shards[shard_index] = Some(Arc::clone(prior.shard(shard_index)));
                shard_index = shard_index.saturating_add(1);
                fragment_index = 0;
                after_series_id = None;
                continue;
            }
            let fragment = &self.fragments_by_shard[shard_index][fragment_index];
            let next = fragment
                .range((after_series_id.map_or(Unbounded, Excluded), Unbounded))
                .next();
            let Some((&series_id, additional)) = next else {
                fragment_index = fragment_index.saturating_add(1);
                after_series_id = None;
                continue;
            };
            let work_bytes = Self::change_check_work_bytes(additional);
            if !budget.charge_for(REMOTE_TOMBSTONE_CHANGE_CHECK_OPERATION, 1, work_bytes)? {
                return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
            }
            let covered = prior
                .shard(shard_index)
                .get(&series_id)
                .is_some_and(|ranges| {
                    additional.iter().all(|range| {
                        tombstone::exclusive_interval_fully_tombstoned(
                            range.start,
                            range.end,
                            ranges,
                        )
                    })
                });
            if covered {
                self.phase = BoundedRemoteTombstoneRefreshPhase::CheckingShardFragments {
                    shard_index,
                    fragment_index,
                    after_series_id: Some(series_id),
                };
            } else {
                self.candidate_changed = true;
                self.phase = BoundedRemoteTombstoneRefreshPhase::CopyingBaseShard {
                    shard_index,
                    after_series_id: None,
                };
            }
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
        }
    }

    fn advance_base_shard(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        shard_index: usize,
        after_series_id: Option<SeriesId>,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        let prior = self.prior_remote_snapshot.as_ref().ok_or_else(|| {
            TsinkError::DataCorruption(
                "finite remote tombstone candidate lost its pinned live snapshot".to_string(),
            )
        })?;
        let next = prior
            .shard(shard_index)
            .range((after_series_id.map_or(Unbounded, Excluded), Unbounded));
        let Some((&series_id, ranges)) = next.into_iter().next() else {
            self.phase = BoundedRemoteTombstoneRefreshPhase::MergingShardFragments {
                shard_index,
                fragment_index: 0,
            };
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
        };
        let work_bytes = Self::base_entry_work_bytes(ranges);
        if !budget.charge_for(REMOTE_TOMBSTONE_BASE_ENTRY_OPERATION, 1, work_bytes)? {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        let work_bytes_usize =
            usize::try_from(work_bytes).map_err(|_| TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_BASE_ENTRY_OPERATION,
                limit: usize::MAX as u64,
                required: work_bytes,
            })?;
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(work_bytes_usize)?;
        let copied = ranges.clone();
        self.current_candidate_shard_memory_bytes = self
            .current_candidate_shard_memory_bytes
            .saturating_add(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES)
            .saturating_add(
                copied
                    .capacity()
                    .saturating_mul(mem::size_of::<tombstone::TombstoneRange>()),
            );
        self.current_candidate_shard.insert(series_id, copied);
        self.phase = BoundedRemoteTombstoneRefreshPhase::CopyingBaseShard {
            shard_index,
            after_series_id: Some(series_id),
        };
        self.retain_operation_bytes(storage, work_bytes, &mut reservation)?;
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn advance_shard_merge(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        shard_index: usize,
        mut fragment_index: usize,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        while fragment_index < self.fragments_by_shard[shard_index].len()
            && self.fragments_by_shard[shard_index][fragment_index].is_empty()
        {
            fragment_index = fragment_index.saturating_add(1);
        }
        if fragment_index == self.fragments_by_shard[shard_index].len() {
            let entries = mem::take(&mut self.current_candidate_shard);
            let memory_usage_bytes = mem::take(&mut self.current_candidate_shard_memory_bytes);
            self.candidate_shards[shard_index] = Some(
                tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(
                    entries,
                    memory_usage_bytes,
                ),
            );
            self.phase = BoundedRemoteTombstoneRefreshPhase::CheckingShardFragments {
                shard_index: shard_index.saturating_add(1),
                fragment_index: 0,
                after_series_id: None,
            };
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed);
        }

        let (&series_id, additional) = self.fragments_by_shard[shard_index][fragment_index]
            .first_key_value()
            .expect("empty fragments were skipped above");
        let current = self
            .current_candidate_shard
            .get(&series_id)
            .map(Vec::as_slice);
        let work_bytes = Self::merge_entry_work_bytes(current, additional);
        if !budget.charge_for(REMOTE_TOMBSTONE_MERGE_ENTRY_OPERATION, 1, work_bytes)? {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred);
        }
        let work_bytes_usize =
            usize::try_from(work_bytes).map_err(|_| TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_MERGE_ENTRY_OPERATION,
                limit: usize::MAX as u64,
                required: work_bytes,
            })?;
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(work_bytes_usize)?;

        let (_, additional) = self.fragments_by_shard[shard_index][fragment_index]
            .pop_first()
            .expect("peeked fragment entry remains present");
        let had_entry = self.current_candidate_shard.contains_key(&series_id);
        let before_capacity = self
            .current_candidate_shard
            .get(&series_id)
            .map_or(0, Vec::capacity);
        let ranges = self.current_candidate_shard.entry(series_id).or_default();
        let _ = tombstone::merge_normalized_tombstone_ranges_owned(ranges, additional);
        let after_capacity = ranges.capacity();
        self.current_candidate_shard_memory_bytes = self
            .current_candidate_shard_memory_bytes
            .saturating_sub(
                before_capacity.saturating_mul(mem::size_of::<tombstone::TombstoneRange>()),
            )
            .saturating_add(
                after_capacity.saturating_mul(mem::size_of::<tombstone::TombstoneRange>()),
            );
        if !had_entry {
            self.current_candidate_shard_memory_bytes = self
                .current_candidate_shard_memory_bytes
                .saturating_add(tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES);
        }
        self.phase = BoundedRemoteTombstoneRefreshPhase::MergingShardFragments {
            shard_index,
            fragment_index,
        };
        self.retain_operation_bytes(storage, work_bytes, &mut reservation)?;
        Ok(BoundedRemoteTombstoneRefreshOutcome::Progressed)
    }

    fn publish(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        expected_visibility_generation: u64,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        let publication_bytes =
            usize::try_from(REMOTE_TOMBSTONE_PUBLICATION_FIXED_BYTES).map_err(|_| {
                TsinkError::MaintenanceWorkItemTooLarge {
                    operation: REMOTE_TOMBSTONE_PUBLICATION_OPERATION,
                    limit: usize::MAX as u64,
                    required: REMOTE_TOMBSTONE_PUBLICATION_FIXED_BYTES,
                }
            })?;
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.ensure(publication_bytes)?;

        let _visibility_guard = storage.visibility_write_fence();
        if storage.visibility_state_generation() != expected_visibility_generation {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart);
        }
        match self.terminal_revalidate_manifests(
            storage,
            budget,
            &mut reservation,
            publication_bytes,
            1,
            REMOTE_TOMBSTONE_PUBLICATION_FIXED_BYTES,
            REMOTE_TOMBSTONE_PUBLICATION_OPERATION,
        )? {
            TerminalManifestRevalidation::Deferred => {
                return Ok(BoundedRemoteTombstoneRefreshOutcome::Deferred)
            }
            TerminalManifestRevalidation::Changed => {
                return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart)
            }
            TerminalManifestRevalidation::Validated => {}
        }
        let shards = mem::take(&mut self.candidate_shards)
            .into_iter()
            .map(|shard| {
                shard.ok_or_else(|| {
                    TsinkError::DataCorruption(
                        "finite remote tombstone publication lost a candidate shard".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let candidate = Arc::new(tombstone::ImmutableTombstoneSnapshot::from_shards(shards));
        // The live pointer still owns the predecessor until the atomic publication below. Drop
        // the cursor's extra predecessor Arc while its staged charge remains in force, so the
        // swap can debit old live bytes without leaving another unaccounted owner reachable.
        drop(self.prior_remote_snapshot.take());
        let visibility_generation = storage
            .tombstone_publication_context()
            .publish_remote_tombstones_locked(storage, candidate)?;
        self.release_retained_bytes(storage);
        Ok(BoundedRemoteTombstoneRefreshOutcome::Published {
            visibility_generation,
        })
    }

    pub(super) fn advance(
        &mut self,
        storage: &ChunkStorage,
        budget: &mut RemoteCatalogPassBudget,
        expected_visibility_generation: u64,
    ) -> Result<BoundedRemoteTombstoneRefreshOutcome> {
        if matches!(
            self.phase,
            BoundedRemoteTombstoneRefreshPhase::CopyingBaseShard { .. }
                | BoundedRemoteTombstoneRefreshPhase::CheckingShardFragments { .. }
                | BoundedRemoteTombstoneRefreshPhase::MergingShardFragments { .. }
                | BoundedRemoteTombstoneRefreshPhase::Publishing
        ) && storage.visibility_state_generation() != expected_visibility_generation
        {
            return Ok(BoundedRemoteTombstoneRefreshOutcome::Restart);
        }
        match self.phase {
            BoundedRemoteTombstoneRefreshPhase::Initializing => self.advance_initialization(budget),
            BoundedRemoteTombstoneRefreshPhase::Manifests { lane_index } => {
                self.advance_manifest(storage, budget, lane_index)
            }
            BoundedRemoteTombstoneRefreshPhase::Shards {
                lane_index,
                shard_index,
            } => self.advance_shard(storage, budget, lane_index, shard_index),
            BoundedRemoteTombstoneRefreshPhase::Revalidating { lane_index } => {
                self.advance_revalidation(storage, budget, lane_index)
            }
            BoundedRemoteTombstoneRefreshPhase::CandidateSetup => {
                self.setup_candidate(storage, budget, expected_visibility_generation)
            }
            BoundedRemoteTombstoneRefreshPhase::CheckingShardFragments {
                shard_index,
                fragment_index,
                after_series_id,
            } => self.advance_candidate_change_check(
                storage,
                budget,
                shard_index,
                fragment_index,
                after_series_id,
                expected_visibility_generation,
            ),
            BoundedRemoteTombstoneRefreshPhase::CopyingBaseShard {
                shard_index,
                after_series_id,
            } => self.advance_base_shard(storage, budget, shard_index, after_series_id),
            BoundedRemoteTombstoneRefreshPhase::MergingShardFragments {
                shard_index,
                fragment_index,
            } => self.advance_shard_merge(storage, budget, shard_index, fragment_index),
            BoundedRemoteTombstoneRefreshPhase::Publishing => {
                self.publish(storage, budget, expected_visibility_generation)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::super::super::super::config::{ChunkStorageOptions, TieredStorageConfig};
    use super::super::super::super::tiering::{
        self, PersistedSegmentTier, SegmentInventory, SegmentLaneFamily,
    };
    use super::*;

    fn test_tiered_storage(root: &Path) -> TieredStorageConfig {
        TieredStorageConfig {
            object_store_root: root.to_path_buf(),
            segment_catalog_path: None,
            mirror_hot_segments: false,
            hot_retention_window: 10,
            warm_retention_window: 50,
        }
    }

    fn test_storage(
        tiered_storage: TieredStorageConfig,
        max_items: usize,
        max_bytes: u64,
    ) -> ChunkStorage {
        tiering::persist_shared_segment_catalog_budgeted(
            &tiered_storage,
            &SegmentInventory::from_entries(Vec::new()),
            None,
        )
        .unwrap();
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                runtime_mode: StorageRuntimeMode::ComputeOnly,
                retention_enforced: false,
                maintenance_max_items_per_pass: max_items,
                maintenance_max_bytes_per_pass: max_bytes,
                remote_segment_refresh_interval: Duration::from_millis(1),
                tiered_storage: Some(tiered_storage),
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap()
    }

    fn hot_numeric_tombstone_path(tiered_storage: &TieredStorageConfig) -> PathBuf {
        tiered_storage
            .lane_path(SegmentLaneFamily::Numeric, PersistedSegmentTier::Hot)
            .join(tombstone::TOMBSTONES_FILE_NAME)
    }

    fn map(entries: &[(u64, i64, i64)]) -> TombstoneMap {
        entries
            .iter()
            .map(|&(series_id, start, end)| {
                (series_id, vec![tombstone::TombstoneRange { start, end }])
            })
            .collect()
    }

    fn immutable_snapshot(entries: TombstoneMap) -> Arc<tombstone::ImmutableTombstoneSnapshot> {
        let mut maps = (0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT)
            .map(|_| TombstoneMap::new())
            .collect::<Vec<_>>();
        for (series_id, ranges) in entries {
            maps[tombstone::ImmutableTombstoneSnapshot::shard_index(series_id)]
                .insert(series_id, ranges);
        }
        let shards = maps
            .into_iter()
            .map(|entries| {
                let memory_usage = ChunkStorage::tombstone_map_memory_usage_bytes(&entries);
                tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(
                    entries,
                    memory_usage,
                )
            })
            .collect();
        Arc::new(tombstone::ImmutableTombstoneSnapshot::from_shards(shards))
    }

    fn publish_remote_snapshot(storage: &ChunkStorage, entries: TombstoneMap) -> u64 {
        let _visibility_guard = storage.visibility_write_fence();
        storage
            .tombstone_publication_context()
            .publish_remote_tombstones_locked(storage, immutable_snapshot(entries))
            .unwrap()
    }

    fn install_live_tombstones(storage: &ChunkStorage, tombstones: TombstoneMap) {
        let mut reservation = storage.tombstone_memory_reservation();
        storage
            .tombstone_publication_context()
            .replace_loaded_tombstones_index(storage, tombstones, &mut reservation)
            .unwrap();
    }

    fn assert_live_tombstones(storage: &ChunkStorage, expected: &TombstoneMap) {
        assert_eq!(storage.tombstone_read_context().snapshot(), *expected);
    }

    fn advance_until(
        storage: &ChunkStorage,
        cycle: &mut BoundedRemoteTombstoneRefreshCycle,
        expected_visibility_generation: u64,
        target: BoundedRemoteTombstoneRefreshPhase,
    ) {
        let mut budget = RemoteCatalogPassBudget::new(128, 256 * 1024 * 1024);
        for _ in 0..64 {
            if cycle.phase == target {
                return;
            }
            assert!(matches!(
                cycle
                    .advance(storage, &mut budget, expected_visibility_generation)
                    .unwrap(),
                BoundedRemoteTombstoneRefreshOutcome::Progressed
            ));
        }
        panic!("finite remote tombstone cursor did not reach {target:?}");
    }

    fn advance_to_publication(
        storage: &ChunkStorage,
        cycle: &mut BoundedRemoteTombstoneRefreshCycle,
        expected_visibility_generation: u64,
    ) -> u64 {
        for _ in 0..1_024 {
            let mut budget = RemoteCatalogPassBudget::new(1_024, u64::MAX);
            match cycle
                .advance(storage, &mut budget, expected_visibility_generation)
                .unwrap()
            {
                BoundedRemoteTombstoneRefreshOutcome::Published {
                    visibility_generation,
                } => return visibility_generation,
                BoundedRemoteTombstoneRefreshOutcome::Progressed => {}
                BoundedRemoteTombstoneRefreshOutcome::Deferred => {
                    panic!("generous publication driver unexpectedly deferred")
                }
                BoundedRemoteTombstoneRefreshOutcome::Restart => {
                    panic!("stable publication driver unexpectedly restarted")
                }
            }
        }
        panic!("finite remote tombstone cycle did not converge");
    }

    #[test]
    fn local_base_and_remote_overlay_union_overlapping_ranges_per_series() {
        let object_store = TempDir::new().unwrap();
        let storage = test_storage(
            test_tiered_storage(object_store.path()),
            usize::MAX,
            u64::MAX,
        );
        install_live_tombstones(&storage, map(&[(7, 0, 10)]));
        publish_remote_snapshot(&storage, map(&[(7, 5, 20), (263, 30, 40)]));

        storage
            .tombstone_read_context()
            .with_series_tombstone_ranges_for_query(7, None, |ranges| {
                assert_eq!(
                    ranges,
                    Some([tombstone::TombstoneRange { start: 0, end: 20 },].as_slice())
                );
                Ok(())
            })
            .unwrap();
        assert_live_tombstones(&storage, &map(&[(7, 0, 20), (263, 30, 40)]));
        storage.close().unwrap();
    }

    #[test]
    fn all_missing_cursor_initialization_has_exact_global_and_maintenance_boundaries() {
        let object_store = TempDir::new().unwrap();
        let storage = test_storage(
            test_tiered_storage(object_store.path()),
            usize::MAX,
            u64::MAX,
        );
        let lanes = storage
            .tombstone_index_context()
            .remote_tombstone_refresh_lanes()
            .unwrap();
        let required = BoundedRemoteTombstoneRefreshCycle::modeled_initial_retained_bytes(&lanes);
        let used = u64::try_from(storage.memory_used_value()).unwrap();

        storage.memory.budget_bytes.store(
            used.saturating_add(required).saturating_sub(1),
            Ordering::Release,
        );
        let rejected = match BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes.clone()) {
            Ok(_) => panic!("N-1 global bytes must reject cursor construction"),
            Err(err) => err,
        };
        assert!(matches!(
            rejected,
            TsinkError::MemoryBudgetExceeded { budget, required: observed }
                if budget == usize::try_from(used + required - 1).unwrap()
                    && observed == usize::try_from(used + required).unwrap()
        ));
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0,
        );

        storage
            .memory
            .budget_bytes
            .store(used.saturating_add(required), Ordering::Release);
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes).unwrap();
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            required,
        );
        let generation = storage.visibility_state_generation();
        let epoch = storage.remote_tombstone_epoch();

        let mut short = RemoteCatalogPassBudget::new(1, required.saturating_sub(1));
        assert!(matches!(
            cycle.advance(&storage, &mut short, generation).unwrap_err(),
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_CURSOR_INITIALIZATION_OPERATION,
                limit,
                required: observed,
            } if limit == required - 1 && observed == required
        ));
        assert_eq!(
            cycle.phase,
            BoundedRemoteTombstoneRefreshPhase::Initializing,
        );

        let mut exact = RemoteCatalogPassBudget::new(1, required);
        assert!(matches!(
            cycle.advance(&storage, &mut exact, generation).unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Progressed
        ));
        assert_eq!(
            cycle.phase,
            BoundedRemoteTombstoneRefreshPhase::Manifests { lane_index: 0 },
        );

        storage
            .memory
            .budget_bytes
            .store(u64::MAX, Ordering::Release);
        let mut published = false;
        for _ in 0..64 {
            let mut budget = RemoteCatalogPassBudget::new(1, u64::MAX);
            if matches!(
                cycle.advance(&storage, &mut budget, generation).unwrap(),
                BoundedRemoteTombstoneRefreshOutcome::Published { .. }
            ) {
                published = true;
                break;
            }
        }
        assert!(
            published,
            "all-missing cycle must converge without a payload item"
        );
        assert_eq!(storage.remote_tombstone_epoch(), epoch);
        assert_eq!(storage.visibility_state_generation(), generation);
        assert!(cycle.has_no_retained_bytes());
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0,
        );
        storage.close().unwrap();
    }

    #[test]
    fn retained_charge_is_released_only_after_cursor_allocations_are_dropped() {
        let object_store = TempDir::new().unwrap();
        let storage = Arc::new(test_storage(
            test_tiered_storage(object_store.path()),
            usize::MAX,
            u64::MAX,
        ));
        let lanes = storage
            .tombstone_index_context()
            .remote_tombstone_refresh_lanes()
            .unwrap();
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes).unwrap();
        let used = u64::try_from(storage.memory_used_value()).unwrap();
        storage.memory.budget_bytes.store(used, Ordering::Release);

        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let continue_rx = std::sync::Mutex::new(continue_rx);
        cycle.release_after_drop_hook = Some(Arc::new(move || {
            dropped_tx.send(()).unwrap();
            continue_rx.lock().unwrap().recv().unwrap();
        }));
        let release_storage = Arc::clone(&storage);
        let release = std::thread::spawn(move || {
            cycle.release_retained_bytes(&release_storage);
        });

        dropped_rx.recv().unwrap();
        let mut blocked = storage.tombstone_memory_reservation();
        assert!(matches!(
            blocked.ensure(1).unwrap_err(),
            TsinkError::MemoryBudgetExceeded { .. }
        ));
        continue_tx.send(()).unwrap();
        release.join().unwrap();
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0,
        );

        let mut admitted = storage.tombstone_memory_reservation();
        admitted.ensure(1).unwrap();
        drop(admitted);
        storage
            .memory
            .budget_bytes
            .store(u64::MAX, Ordering::Release);
        storage.close().unwrap();
    }

    #[test]
    fn cache_epoch_invalidates_logically_and_visibility_fence_blocks_mid_read_swap() {
        use std::sync::mpsc;

        let object_store = TempDir::new().unwrap();
        let storage = Arc::new(test_storage(
            test_tiered_storage(object_store.path()),
            usize::MAX,
            u64::MAX,
        ));
        let series_id = 7;
        storage
            .visibility
            .series_visibility_summaries
            .write()
            .insert(
                series_id,
                SeriesVisibilitySummary {
                    latest_visible_timestamp: Some(100),
                    latest_bounded_visible_timestamp: Some(100),
                    ..SeriesVisibilitySummary::default()
                },
            );
        storage
            .visibility
            .series_visible_max_timestamps
            .write()
            .insert(series_id, Some(100));
        storage
            .visibility
            .series_visible_bounded_max_timestamps
            .write()
            .insert(series_id, Some(100));
        storage
            .visibility
            .series_visibility_cache_epochs
            .write()
            .insert(series_id, storage.remote_tombstone_epoch());

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let reader_storage = Arc::clone(&storage);
        let reader = std::thread::spawn(move || {
            reader_storage.with_series_visibility_summaries(|summaries| {
                assert!(summaries.contains_key(&series_id));
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                assert!(
                    summaries.contains_key(&series_id),
                    "the read fence keeps the old cache/snapshot pair coherent"
                );
            });
        });
        entered_rx.recv().unwrap();

        let (published_tx, published_rx) = mpsc::channel();
        let publisher_storage = Arc::clone(&storage);
        let publisher = std::thread::spawn(move || {
            publish_remote_snapshot(&publisher_storage, map(&[(series_id, 0, 200)]));
            published_tx.send(()).unwrap();
        });
        assert_eq!(
            published_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout),
            "remote publication must wait while a reader is deciding from an old epoch"
        );
        release_tx.send(()).unwrap();
        reader.join().unwrap();
        published_rx.recv().unwrap();
        publisher.join().unwrap();

        assert!(
            storage
                .visibility
                .series_visibility_summaries
                .read()
                .contains_key(&series_id),
            "epoch invalidation leaves the old allocation in place"
        );
        storage.with_series_visibility_summaries(|summaries| {
            assert!(
                !summaries.contains_key(&series_id),
                "an old physical cache entry is logically absent in the new epoch"
            );
        });
        storage
            .refresh_series_visible_timestamp_cache(std::iter::once(series_id))
            .unwrap();
        storage.with_series_visibility_summaries(|summaries| {
            assert!(
                summaries.contains_key(&series_id),
                "the requested series is lazily retagged after recomputation"
            );
        });
        storage.close().unwrap();
    }

    #[test]
    fn one_series_post_epoch_cache_mutations_visit_only_the_touched_entry() {
        let object_store = TempDir::new().unwrap();
        let storage = test_storage(
            test_tiered_storage(object_store.path()),
            usize::MAX,
            u64::MAX,
        );
        let epoch = storage.remote_tombstone_epoch();
        for series_id in 1..=4_096 {
            storage
                .visibility
                .series_visibility_summaries
                .write()
                .insert(
                    series_id,
                    SeriesVisibilitySummary {
                        latest_visible_timestamp: Some(series_id as i64),
                        latest_bounded_visible_timestamp: Some(series_id as i64),
                        ranges: vec![SeriesVisibilityRangeSummary {
                            min_ts: series_id as i64,
                            max_ts: series_id as i64,
                            exact: true,
                        }],
                        ..SeriesVisibilitySummary::default()
                    },
                );
            storage
                .visibility
                .series_visible_max_timestamps
                .write()
                .insert(series_id, Some(series_id as i64));
            storage
                .visibility
                .series_visible_bounded_max_timestamps
                .write()
                .insert(series_id, Some(series_id as i64));
            storage
                .visibility
                .series_visibility_cache_epochs
                .write()
                .insert(series_id, epoch);
        }
        storage
            .visibility
            .max_bounded_observed_timestamp
            .store(4_096, Ordering::Release);
        publish_remote_snapshot(&storage, map(&[(9_000, 0, 1)]));
        assert_eq!(
            storage.bounded_recency_reference_timestamp(),
            Some(4_096),
            "epoch invalidation must retain the safe monotonic recency upper bound"
        );

        storage
            .visibility
            .visibility_cache_accounting_entries_visited
            .store(0, Ordering::Release);
        storage.clear_series_visible_timestamp_cache(std::iter::once(1));
        assert_eq!(
            storage
                .visibility
                .visibility_cache_accounting_entries_visited
                .load(Ordering::Acquire),
            1,
        );

        storage
            .visibility
            .visibility_cache_accounting_entries_visited
            .store(0, Ordering::Release);
        storage
            .refresh_series_visible_timestamp_cache(std::iter::once(2))
            .unwrap();
        assert_eq!(
            storage
                .visibility
                .visibility_cache_accounting_entries_visited
                .load(Ordering::Acquire),
            1,
        );
        assert!(
            storage.visibility.series_visibility_summaries.read().len() >= 4_095,
            "unrelated stale cache payloads remain physically allocated and unvisited"
        );
        storage.close().unwrap();
    }

    #[test]
    fn remote_epoch_publication_preserves_retention_reference_for_unaffected_future_data() {
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 100,
                future_skew_window: 1_000,
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                background_threads_enabled: false,
                current_time_override: Some(1_000),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        storage
            .insert_rows(&[Row::new("future_reference", DataPoint::new(1_500, 1.0))])
            .unwrap();
        assert_eq!(storage.bounded_recency_reference_timestamp(), Some(1_500));
        assert!(matches!(
            storage
                .insert_rows(&[Row::new("too_old_before", DataPoint::new(1_300, 1.0))])
                .unwrap_err(),
            TsinkError::OutOfRetention { timestamp: 1_300 }
        ));

        publish_remote_snapshot(&storage, map(&[(9_000, 0, 1)]));
        assert_eq!(storage.bounded_recency_reference_timestamp(), Some(1_500));
        assert!(matches!(
            storage
                .insert_rows(&[Row::new("too_old_after", DataPoint::new(1_300, 1.0))])
                .unwrap_err(),
            TsinkError::OutOfRetention { timestamp: 1_300 }
        ));
        storage.close().unwrap();
    }

    #[test]
    fn exhausted_remote_epoch_rejects_before_any_publication_state_changes() {
        let object_store = TempDir::new().unwrap();
        let storage = test_storage(
            test_tiered_storage(object_store.path()),
            usize::MAX,
            u64::MAX,
        );
        let cached_series_id = 17;
        storage
            .visibility
            .series_visibility_summaries
            .write()
            .insert(
                cached_series_id,
                SeriesVisibilitySummary {
                    latest_visible_timestamp: Some(123),
                    latest_bounded_visible_timestamp: Some(123),
                    ..SeriesVisibilitySummary::default()
                },
            );
        storage
            .visibility
            .series_visibility_cache_epochs
            .write()
            .insert(cached_series_id, 41);
        storage
            .visibility
            .remote_tombstone_epoch
            .store(u64::MAX, Ordering::Release);

        let prior_snapshot = storage.tombstone_read_context().remote_snapshot();
        let prior_registry_next = storage
            .catalog
            .registry
            .read()
            .next_series_id_value_for_test();
        let prior_tombstone_bytes = storage.memory.tombstone_used_bytes.load(Ordering::Acquire);
        let prior_staged_bytes = storage
            .memory
            .tombstone_staged_bytes
            .load(Ordering::Acquire);
        let prior_visibility_generation = storage.visibility_state_generation();
        let prior_tombstone_generation = storage.tombstone_state_generation();
        let prior_pruning_generation = storage.live_series_pruning_generation();
        let candidate_series_id = prior_registry_next.saturating_add(10_000);
        let candidate = immutable_snapshot(map(&[(candidate_series_id, 10, 20)]));

        let _visibility_guard = storage.visibility_write_fence();
        let error = storage
            .tombstone_publication_context()
            .publish_remote_tombstones_locked(&storage, candidate)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("remote tombstone visibility epoch exhausted"));
        assert!(Arc::ptr_eq(
            &prior_snapshot,
            &storage.tombstone_read_context().remote_snapshot(),
        ));
        assert_eq!(
            storage
                .catalog
                .registry
                .read()
                .next_series_id_value_for_test(),
            prior_registry_next,
        );
        assert_eq!(storage.remote_tombstone_epoch(), u64::MAX);
        assert_eq!(
            storage.memory.tombstone_used_bytes.load(Ordering::Acquire),
            prior_tombstone_bytes,
        );
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            prior_staged_bytes,
        );
        assert_eq!(
            storage.visibility_state_generation(),
            prior_visibility_generation
        );
        assert_eq!(
            storage.tombstone_state_generation(),
            prior_tombstone_generation
        );
        assert_eq!(
            storage.live_series_pruning_generation(),
            prior_pruning_generation
        );
        assert_eq!(
            storage
                .visibility
                .series_visibility_cache_epochs
                .read()
                .get(&cached_series_id),
            Some(&41),
        );
        let cached = storage.visibility.series_visibility_summaries.read();
        assert_eq!(
            cached
                .get(&cached_series_id)
                .and_then(|summary| summary.latest_visible_timestamp),
            Some(123),
        );
        drop(cached);
        drop(_visibility_guard);
        storage.close().unwrap();
    }

    #[test]
    fn identical_second_refresh_preserves_pointer_epochs_generations_and_cache() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        tombstone::persist_tombstones(
            &hot_numeric_tombstone_path(&tiered_storage),
            &map(&[(1, 10, 20)]),
        )
        .unwrap();

        let generation = storage.visibility_state_generation();
        let lanes = storage
            .tombstone_index_context()
            .remote_tombstone_refresh_lanes()
            .unwrap();
        let mut first = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes.clone()).unwrap();
        let published_generation = advance_to_publication(&storage, &mut first, generation);
        assert!(published_generation > generation);

        let pointer = storage.tombstone_read_context().remote_snapshot();
        let epoch = storage.remote_tombstone_epoch();
        let visibility_generation = storage.visibility_state_generation();
        let tombstone_generation = storage.tombstone_state_generation();
        let pruning_generation = storage.live_series_pruning_generation();
        storage
            .visibility
            .series_visibility_summaries
            .write()
            .insert(1, SeriesVisibilitySummary::default());
        storage
            .visibility
            .series_visibility_cache_epochs
            .write()
            .insert(1, epoch);

        let mut second = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes).unwrap();
        assert_eq!(
            advance_to_publication(&storage, &mut second, visibility_generation),
            visibility_generation,
        );
        let unchanged_pointer = storage.tombstone_read_context().remote_snapshot();
        assert!(Arc::ptr_eq(&pointer, &unchanged_pointer));
        assert_eq!(storage.remote_tombstone_epoch(), epoch);
        assert_eq!(storage.visibility_state_generation(), visibility_generation);
        assert_eq!(storage.tombstone_state_generation(), tombstone_generation);
        assert_eq!(storage.live_series_pruning_generation(), pruning_generation);
        storage.with_series_visibility_summaries(|summaries| {
            assert!(summaries.contains_key(&1));
        });
        storage.close().unwrap();
    }

    #[test]
    fn exact_item_pages_keep_old_visibility_until_terminal_publication() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), 1, 256 * 1024 * 1024);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        let remote = map(&[(1, 10, 20)]);
        tombstone::persist_tombstones(&hot_numeric_tombstone_path(&tiered_storage), &remote)
            .unwrap();

        let full_scans = Arc::new(AtomicUsize::new(0));
        storage.set_full_inventory_scan_hook({
            let full_scans = Arc::clone(&full_scans);
            move || {
                full_scans.fetch_add(1, Ordering::SeqCst);
            }
        });

        let mut wakes = 0usize;
        loop {
            wakes = wakes.saturating_add(1);
            storage
                .sync_persisted_segments_from_disk_if_dirty()
                .unwrap();
            let observed = storage.tombstone_read_context().snapshot();
            let mut expected = old_live.clone();
            expected.extend(remote.clone());
            if observed == expected {
                break;
            }
            assert_eq!(
                observed, old_live,
                "private shard construction must not leak partial visibility"
            );
            assert!(wakes < 16, "finite one-item refresh failed to converge");
        }
        assert!(
            wakes >= 5,
            "manifest, shard, revalidation, candidate, series, and pointer work must page"
        );
        assert_eq!(full_scans.load(Ordering::SeqCst), 0);
        storage.clear_full_inventory_scan_hook();
        storage.close().unwrap();
    }

    #[test]
    fn publication_byte_limit_rejects_n_plus_one_without_changing_live_map() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        tombstone::persist_tombstones(
            &hot_numeric_tombstone_path(&tiered_storage),
            &map(&[(1, 10, 20)]),
        )
        .unwrap();
        let expected_visibility_generation = storage.visibility_state_generation();
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(
            &storage,
            storage
                .tombstone_index_context()
                .remote_tombstone_refresh_lanes()
                .unwrap(),
        )
        .unwrap();
        advance_until(
            &storage,
            &mut cycle,
            expected_visibility_generation,
            BoundedRemoteTombstoneRefreshPhase::Publishing,
        );
        let required = cycle.terminal_publication_work_bytes_for_test();
        let mut short_budget = RemoteCatalogPassBudget::new(1, required - 1);
        let error = cycle
            .advance(&storage, &mut short_budget, expected_visibility_generation)
            .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_PUBLICATION_OPERATION,
                limit,
                required: observed,
            } if limit == required - 1 && observed == required
        ));
        assert_live_tombstones(&storage, &old_live);

        let mut exact_budget = RemoteCatalogPassBudget::new(1, required);
        assert!(matches!(
            cycle
                .advance(&storage, &mut exact_budget, expected_visibility_generation,)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Published { .. }
        ));
        let expected = map(&[(900, 0, 5), (1, 10, 20)]);
        assert_live_tombstones(&storage, &expected);
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
        storage.close().unwrap();
    }

    #[test]
    fn candidate_setup_and_series_merge_have_exact_item_byte_and_memory_boundaries() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        tombstone::persist_tombstones(
            &hot_numeric_tombstone_path(&tiered_storage),
            &map(&[(1, 10, 20)]),
        )
        .unwrap();
        let expected_visibility_generation = storage.visibility_state_generation();
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(
            &storage,
            storage
                .tombstone_index_context()
                .remote_tombstone_refresh_lanes()
                .unwrap(),
        )
        .unwrap();
        advance_until(
            &storage,
            &mut cycle,
            expected_visibility_generation,
            BoundedRemoteTombstoneRefreshPhase::CandidateSetup,
        );

        let retained_before = storage
            .memory
            .tombstone_staged_bytes
            .load(Ordering::Acquire);
        let exact_memory = storage
            .memory_used_value()
            .saturating_add(REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES as usize);
        storage
            .memory
            .budget_bytes
            .store((exact_memory - 1) as u64, Ordering::Release);
        let mut generous_pass = RemoteCatalogPassBudget::new(1, u64::MAX);
        assert!(matches!(
            cycle
                .advance(
                    &storage,
                    &mut generous_pass,
                    expected_visibility_generation
                )
                .unwrap_err(),
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == exact_memory - 1 && required == exact_memory
        ));
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            retained_before,
            "failed setup admission must not retain a partial candidate"
        );

        storage
            .memory
            .budget_bytes
            .store(exact_memory as u64, Ordering::Release);
        let mut exact_setup_pass =
            RemoteCatalogPassBudget::new(1, REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES);
        assert!(matches!(
            cycle
                .advance(
                    &storage,
                    &mut exact_setup_pass,
                    expected_visibility_generation
                )
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Progressed
        ));
        storage
            .memory
            .budget_bytes
            .store(u64::MAX, Ordering::Release);

        // The first admitted entry check proves the decoded fragment changes the empty prior
        // shard; the following zero-allocation cursor transition reaches fragment merging.
        let mut transition = RemoteCatalogPassBudget::new(1, u64::MAX);
        assert!(matches!(
            cycle
                .advance(&storage, &mut transition, expected_visibility_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Progressed
        ));
        let mut transition = RemoteCatalogPassBudget::new(1, u64::MAX);
        assert!(matches!(
            cycle
                .advance(&storage, &mut transition, expected_visibility_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Progressed
        ));
        let BoundedRemoteTombstoneRefreshPhase::MergingShardFragments {
            shard_index,
            fragment_index,
        } = cycle.phase
        else {
            panic!("empty prior shard should transition to fragment merging");
        };
        let (&series_id, ranges) = cycle.fragments_by_shard[shard_index][fragment_index]
            .first_key_value()
            .unwrap();
        let merge_required = BoundedRemoteTombstoneRefreshCycle::merge_entry_work_bytes(
            cycle
                .current_candidate_shard
                .get(&series_id)
                .map(Vec::as_slice),
            ranges,
        );

        let mut zero_items = RemoteCatalogPassBudget::new(0, merge_required);
        assert!(matches!(
            cycle
                .advance(&storage, &mut zero_items, expected_visibility_generation)
                .unwrap_err(),
            TsinkError::MaintenanceDependencyWindowExceeded {
                operation: REMOTE_TOMBSTONE_MERGE_ENTRY_OPERATION,
                item_limit: 0,
                byte_limit,
                selected_items: 0,
                selected_bytes: 0,
            }
                if byte_limit == merge_required
        ));
        let mut short_bytes = RemoteCatalogPassBudget::new(1, merge_required - 1);
        assert!(matches!(
            cycle
                .advance(&storage, &mut short_bytes, expected_visibility_generation)
                .unwrap_err(),
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_TOMBSTONE_MERGE_ENTRY_OPERATION,
                limit,
                required,
            } if limit == merge_required - 1 && required == merge_required
        ));
        assert!(
            cycle.current_candidate_shard.is_empty(),
            "N-1 must not consume or publish the source entry"
        );
        assert!(cycle.fragments_by_shard[shard_index][fragment_index].contains_key(&series_id));

        let mut exact_merge = RemoteCatalogPassBudget::new(1, merge_required);
        assert!(matches!(
            cycle
                .advance(&storage, &mut exact_merge, expected_visibility_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Progressed
        ));
        assert!(cycle.current_candidate_shard.contains_key(&series_id));
        assert_live_tombstones(&storage, &old_live);
        cycle.release_retained_bytes(&storage);
        drop(cycle);
        storage.close().unwrap();
    }

    #[test]
    fn changed_terminal_manifest_restarts_and_retry_publishes_only_complete_snapshot() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        let tombstone_path = hot_numeric_tombstone_path(&tiered_storage);
        tombstone::persist_tombstones(&tombstone_path, &map(&[(1, 10, 20)])).unwrap();
        let expected_visibility_generation = storage.visibility_state_generation();
        let lanes = storage
            .tombstone_index_context()
            .remote_tombstone_refresh_lanes()
            .unwrap();
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes.clone()).unwrap();
        advance_until(
            &storage,
            &mut cycle,
            expected_visibility_generation,
            BoundedRemoteTombstoneRefreshPhase::Revalidating { lane_index: 0 },
        );

        tombstone::persist_tombstones(&tombstone_path, &map(&[(1, 10, 20), (257, 30, 40)]))
            .unwrap();
        let mut budget = RemoteCatalogPassBudget::new(1, 256 * 1024 * 1024);
        assert!(matches!(
            cycle
                .advance(&storage, &mut budget, expected_visibility_generation,)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Restart
        ));
        assert_live_tombstones(&storage, &old_live);
        cycle.release_retained_bytes(&storage);

        let mut retry = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes).unwrap();
        advance_until(
            &storage,
            &mut retry,
            expected_visibility_generation,
            BoundedRemoteTombstoneRefreshPhase::Publishing,
        );
        let required = retry.terminal_publication_work_bytes_for_test();
        let mut budget = RemoteCatalogPassBudget::new(1, required);
        assert!(matches!(
            retry
                .advance(&storage, &mut budget, expected_visibility_generation,)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Published { .. }
        ));
        assert_live_tombstones(&storage, &map(&[(900, 0, 5), (1, 10, 20), (257, 30, 40)]));
        storage.close().unwrap();
    }

    #[test]
    fn successive_refreshes_preserve_prior_remote_union_and_share_unaffected_shards() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        let tombstone_path = hot_numeric_tombstone_path(&tiered_storage);
        tombstone::persist_tombstones(&tombstone_path, &map(&[(1, 10, 20)])).unwrap();

        let first_generation = storage.visibility_state_generation();
        let mut first = BoundedRemoteTombstoneRefreshCycle::new(
            &storage,
            storage
                .tombstone_index_context()
                .remote_tombstone_refresh_lanes()
                .unwrap(),
        )
        .unwrap();
        advance_until(
            &storage,
            &mut first,
            first_generation,
            BoundedRemoteTombstoneRefreshPhase::Publishing,
        );
        let mut terminal =
            RemoteCatalogPassBudget::new(1, first.terminal_publication_work_bytes_for_test());
        assert!(matches!(
            first
                .advance(&storage, &mut terminal, first_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Published { .. }
        ));
        let first_snapshot = storage.tombstone_read_context().remote_snapshot();
        assert_live_tombstones(&storage, &map(&[(900, 0, 5), (1, 10, 20)]));

        // The remote writer's next authoritative image omits series 1. Compute-only readers
        // nevertheless retain it monotonically and COW-share that unaffected live shard.
        tombstone::persist_tombstones(&tombstone_path, &map(&[(2, 30, 40)])).unwrap();
        let second_generation = storage.visibility_state_generation();
        let mut second = BoundedRemoteTombstoneRefreshCycle::new(
            &storage,
            storage
                .tombstone_index_context()
                .remote_tombstone_refresh_lanes()
                .unwrap(),
        )
        .unwrap();
        advance_until(
            &storage,
            &mut second,
            second_generation,
            BoundedRemoteTombstoneRefreshPhase::Publishing,
        );
        let mut terminal =
            RemoteCatalogPassBudget::new(1, second.terminal_publication_work_bytes_for_test());
        assert!(matches!(
            second
                .advance(&storage, &mut terminal, second_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Published { .. }
        ));
        let second_snapshot = storage.tombstone_read_context().remote_snapshot();
        let prior_shard = tombstone::ImmutableTombstoneSnapshot::shard_index(1);
        assert!(Arc::ptr_eq(
            first_snapshot.shard(prior_shard),
            second_snapshot.shard(prior_shard),
        ));
        assert_live_tombstones(&storage, &map(&[(900, 0, 5), (1, 10, 20), (2, 30, 40)]));
        storage.close().unwrap();
    }

    #[test]
    fn cleaned_pinned_shard_revalidates_changed_manifest_and_restarts() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        let tombstone_path = hot_numeric_tombstone_path(&tiered_storage);
        tombstone::persist_tombstones(&tombstone_path, &map(&[(1, 10, 20)])).unwrap();
        let expected_visibility_generation = storage.visibility_state_generation();
        let lanes = storage
            .tombstone_index_context()
            .remote_tombstone_refresh_lanes()
            .unwrap();
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes.clone()).unwrap();
        advance_until(
            &storage,
            &mut cycle,
            expected_visibility_generation,
            BoundedRemoteTombstoneRefreshPhase::Shards {
                lane_index: 0,
                shard_index: 0,
            },
        );
        let (lane_index, _, old_file_name) = cycle
            .next_referenced_shard(0, 0)
            .expect("the pinned manifest should reference its first shard");
        let old_shard_path = cycle.lanes[lane_index]
            .manifest_path
            .with_file_name(format!("{}.store", tombstone::TOMBSTONES_FILE_NAME))
            .join("shards")
            .join(old_file_name);

        let replacement = map(&[(1, 10, 20), (257, 30, 40)]);
        tombstone::persist_tombstones(&tombstone_path, &replacement).unwrap();
        assert!(
            !old_shard_path.exists(),
            "committed writer cleanup should retire the shard pinned by the old manifest"
        );
        let mut budget = RemoteCatalogPassBudget::new(1, 256 * 1024 * 1024);
        assert!(matches!(
            cycle
                .advance(&storage, &mut budget, expected_visibility_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Restart
        ));
        assert_live_tombstones(&storage, &old_live);
        cycle.release_retained_bytes(&storage);
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );

        let mut retry = BoundedRemoteTombstoneRefreshCycle::new(&storage, lanes).unwrap();
        advance_until(
            &storage,
            &mut retry,
            expected_visibility_generation,
            BoundedRemoteTombstoneRefreshPhase::Publishing,
        );
        let required = retry.terminal_publication_work_bytes_for_test();
        let mut budget = RemoteCatalogPassBudget::new(1, required);
        assert!(matches!(
            retry
                .advance(&storage, &mut budget, expected_visibility_generation)
                .unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Published { .. }
        ));
        let mut expected = old_live;
        expected.extend(replacement);
        assert_live_tombstones(&storage, &expected);
        storage.close().unwrap();
    }

    #[test]
    fn unchanged_corrupt_shard_error_clears_parent_retained_continuation() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), 1, 256 * 1024 * 1024);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        let tombstone_path = hot_numeric_tombstone_path(&tiered_storage);
        tombstone::persist_tombstones(&tombstone_path, &map(&[(1, 10, 20)])).unwrap();

        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire)
                > 0
        );
        let lane = storage
            .tombstone_index_context()
            .remote_tombstone_refresh_lanes()
            .unwrap()
            .into_iter()
            .next()
            .expect("hot numeric remote lane");
        let manifest_len = tombstone::remote_tombstone_manifest_file_len(&lane).unwrap();
        let snapshot =
            tombstone::load_remote_tombstone_manifest_snapshot(&lane, manifest_len).unwrap();
        let RemoteTombstoneManifestKind::Sharded(shards) = snapshot.kind else {
            panic!("test persistence should create a sharded tombstone manifest");
        };
        let shard_name = shards
            .into_iter()
            .flatten()
            .next()
            .expect("test manifest should reference a shard");
        let shard_path = lane
            .manifest_path
            .with_file_name(format!("{}.store", tombstone::TOMBSTONES_FILE_NAME))
            .join("shards")
            .join(shard_name);
        std::fs::remove_file(shard_path).unwrap();

        for _ in 0..16 {
            storage
                .sync_persisted_segments_from_disk_if_dirty()
                .unwrap();
            if storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire)
                == 0
            {
                break;
            }
        }
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0,
            "a propagated shard error must not pin the failed subcycle across backoff"
        );
        assert_live_tombstones(&storage, &old_live);
        storage.close().unwrap();
    }

    #[test]
    fn visibility_churn_discards_partially_built_shards_and_releases_accounting() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), usize::MAX, u64::MAX);
        let old_live = map(&[(900, 0, 5)]);
        install_live_tombstones(&storage, old_live.clone());
        tombstone::persist_tombstones(
            &hot_numeric_tombstone_path(&tiered_storage),
            &map(&[(1, 10, 20)]),
        )
        .unwrap();
        let generation = storage.visibility_state_generation();
        let mut cycle = BoundedRemoteTombstoneRefreshCycle::new(
            &storage,
            storage
                .tombstone_index_context()
                .remote_tombstone_refresh_lanes()
                .unwrap(),
        )
        .unwrap();
        advance_until(
            &storage,
            &mut cycle,
            generation,
            BoundedRemoteTombstoneRefreshPhase::CandidateSetup,
        );
        let mut setup = RemoteCatalogPassBudget::new(1, REMOTE_TOMBSTONE_CANDIDATE_FIXED_BYTES);
        assert!(matches!(
            cycle.advance(&storage, &mut setup, generation).unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Progressed
        ));
        assert!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire)
                > 0
        );

        storage.bump_visibility_state_generation();
        let mut budget = RemoteCatalogPassBudget::new(1, u64::MAX);
        assert!(matches!(
            cycle.advance(&storage, &mut budget, generation).unwrap(),
            BoundedRemoteTombstoneRefreshOutcome::Restart
        ));
        cycle.release_retained_bytes(&storage);
        drop(cycle);
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0
        );
        assert_live_tombstones(&storage, &old_live);
        storage.close().unwrap();
    }

    #[test]
    fn fragment_union_consumes_each_normalized_range_once() {
        const RANGE_COUNT: usize = 20_000;
        let existing = (0..RANGE_COUNT)
            .map(|index| tombstone::TombstoneRange {
                start: i64::try_from(index * 4).unwrap(),
                end: i64::try_from(index * 4 + 1).unwrap(),
            })
            .collect::<Vec<_>>();
        let additional = (0..RANGE_COUNT)
            .map(|index| tombstone::TombstoneRange {
                start: i64::try_from(index * 4 + 2).unwrap(),
                end: i64::try_from(index * 4 + 3).unwrap(),
            })
            .collect::<Vec<_>>();
        let mut candidate = TombstoneMap::from([(7, existing)]);
        let fragment = TombstoneMap::from([(7, additional)]);

        let consumed =
            BoundedRemoteTombstoneRefreshCycle::merge_fragment(&mut candidate, &fragment);

        assert_eq!(consumed, RANGE_COUNT * 2);
        assert_eq!(candidate[&7].len(), RANGE_COUNT * 2);
        assert!(candidate[&7]
            .windows(2)
            .all(|pair| pair[0].end < pair[1].start));
    }

    #[test]
    fn close_releases_a_partially_staged_remote_tombstone_cycle() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        let storage = test_storage(tiered_storage.clone(), 1, 256 * 1024 * 1024);
        tombstone::persist_tombstones(
            &hot_numeric_tombstone_path(&tiered_storage),
            &map(&[(1, 10, 20)]),
        )
        .unwrap();

        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire)
                > 0,
            "the first one-item pass should retain its decoded manifest state"
        );

        storage.close().unwrap();
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0,
            "close must release staged continuation memory even before terminal publication"
        );
    }

    #[test]
    fn failed_close_after_continuation_reset_reopens_with_no_pinned_bytes() {
        let object_store = TempDir::new().unwrap();
        let tiered_storage = test_tiered_storage(object_store.path());
        tiering::persist_shared_segment_catalog_budgeted(
            &tiered_storage,
            &SegmentInventory::from_entries(Vec::new()),
            None,
        )
        .unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                runtime_mode: StorageRuntimeMode::ComputeOnly,
                retention_enforced: false,
                write_timeout: Duration::ZERO,
                maintenance_max_items_per_pass: 1,
                maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
                remote_segment_refresh_interval: Duration::from_millis(1),
                tiered_storage: Some(tiered_storage.clone()),
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        tombstone::persist_tombstones(
            &hot_numeric_tombstone_path(&tiered_storage),
            &map(&[(1, 10, 20)]),
        )
        .unwrap();
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire)
                > 0
        );

        let held_writer = storage.runtime.write_limiter.acquire();
        assert!(matches!(
            storage.close(),
            Err(TsinkError::WriteTimeout { timeout_ms: 0, .. })
        ));
        assert_eq!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire),
            0,
            "a later close timeout must not restore the discarded continuation charge"
        );
        assert_eq!(
            storage.coordination.lifecycle.load(Ordering::Acquire),
            super::super::super::super::STORAGE_OPEN
        );
        drop(held_writer);

        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert!(
            storage
                .memory
                .tombstone_staged_bytes
                .load(Ordering::Acquire)
                > 0,
            "the reopened storage should start a fresh bounded continuation"
        );
        storage.close().unwrap();
    }
}
