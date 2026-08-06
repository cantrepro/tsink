use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::engine::binio::{append_u16, append_u32, checksum32, read_u16, read_u32};
use crate::engine::fs_utils::{remove_file_if_exists, write_file_atomically_and_sync_parent};
use crate::engine::series::SeriesId;
use crate::{Result, TsinkError};

pub(crate) const TOMBSTONES_FILE_NAME: &str = "tombstones.json";
const TOMBSTONES_FILE_VERSION: u16 = 1;
const TOMBSTONE_STORE_VERSION: u16 = 2;
const TOMBSTONE_STORE_SHARD_COUNT: usize = 256;
/// Fixed fanout for the immutable query-visible remote tombstone overlay.
///
/// This intentionally matches the durable v2 store fanout. A refresh can therefore rebuild one
/// logical shard incrementally while every unaffected shard remains shared with the previous
/// snapshot. The fixed fanout also makes the terminal snapshot assembly and pointer swap
/// independent of the number of tombstoned series.
pub(crate) const LIVE_TOMBSTONE_SHARD_COUNT: usize = TOMBSTONE_STORE_SHARD_COUNT;
const TOMBSTONE_STORE_MAGIC: &[u8; 8] = b"TSINKTM2";
const TOMBSTONE_SHARD_MAGIC: &[u8; 8] = b"TSINKTS2";
const TOMBSTONE_STORE_DIR_SUFFIX: &str = ".store";
const TOMBSTONE_SHARDS_DIR_NAME: &str = "shards";
pub(crate) const TOMBSTONE_TRANSACTION_DIR_NAME: &str = ".tombstone-transactions";
pub(crate) const TOMBSTONE_TRANSACTION_FILE_NAME: &str = "active.bin";
const TOMBSTONE_TRANSACTION_MAGIC: [u8; 8] = *b"TSINKTT1";
const TOMBSTONE_TRANSACTION_VERSION: u16 = 1;
const TOMBSTONE_TRANSACTION_HEADER_LEN: usize = 8 + 2 + 4;
const TOMBSTONE_TRANSACTION_CHECKSUM_LEN: usize = 4;
const MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_TOMBSTONE_SHARD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_TOMBSTONE_TRANSACTION_PATH_BYTES: usize = 256 * 1024;
const TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK: usize = 16 * 1024;
const TOMBSTONE_CLEANUP_ENTRY_PATH_BYTES: usize = MAX_TOMBSTONE_TRANSACTION_PATH_BYTES;
const TOMBSTONE_CLEANUP_ENTRY_NAME_BYTES: usize = 4 * 1024;
const TOMBSTONE_CLEANUP_ALLOCATOR_SLACK: usize = 4 * 1024;

static TOMBSTONE_SHARD_FILE_COUNTER: AtomicU64 = AtomicU64::new(1);
static TOMBSTONE_TRANSACTION_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
type TombstoneTransactionHook =
    dyn Fn(TombstoneTransactionTestPoint) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
type TombstoneTransactionHookRegistration = (std::thread::ThreadId, Arc<TombstoneTransactionHook>);

#[cfg(test)]
pub(crate) struct TombstoneTransactionHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for TombstoneTransactionHookGuard {
    fn drop(&mut self) {
        *tombstone_transaction_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(test)]
fn tombstone_transaction_hook_slot(
) -> &'static std::sync::Mutex<Option<TombstoneTransactionHookRegistration>> {
    static HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<TombstoneTransactionHookRegistration>>,
    > = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn tombstone_transaction_test_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn invoke_tombstone_transaction_hook(point: TombstoneTransactionTestPoint) -> Result<()> {
    let current_thread = std::thread::current().id();
    let hook = tombstone_transaction_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .filter(|(owner, _)| *owner == current_thread)
        .map(|(_, hook)| Arc::clone(hook));
    match hook {
        Some(hook) => hook(point),
        None => Ok(()),
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TombstoneTransactionTestPoint {
    BeforeCommitDecision,
    AmbiguousCommitDecision,
    AfterCommitDecision,
    BeforeManifest(usize),
    AfterManifest(usize),
}

#[cfg(test)]
pub(crate) fn fail_tombstone_transaction_once(
    target: TombstoneTransactionTestPoint,
    message: impl Into<String>,
) -> TombstoneTransactionHookGuard {
    use std::sync::atomic::AtomicBool;

    let lock = tombstone_transaction_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let failed = Arc::new(AtomicBool::new(false));
    let message = message.into();
    *tombstone_transaction_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some((
        std::thread::current().id(),
        Arc::new(move |point| {
            if point == target && !failed.swap(true, Ordering::SeqCst) {
                return Err(TsinkError::Other(message.clone()));
            }
            Ok(())
        }),
    ));
    TombstoneTransactionHookGuard { _lock: lock }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TombstoneRange {
    pub(crate) start: i64,
    pub(crate) end: i64,
}

/// Ordered query-visible and persistence staging map.
///
/// Recovery snapshot publication advances by `SeriesId` across bounded maintenance wakes. A
/// `HashMap` iterator cannot be resumed after releasing the visibility fence without retaining a
/// whole-map clone, so the ordered map is part of that bounded-work contract.
pub(crate) type TombstoneMap = BTreeMap<SeriesId, Vec<TombstoneRange>>;

/// Immutable, fixed-fanout overlay used by finite compute-only refresh.
///
/// The mutable `TombstoneMap` remains the durable local/base view. Remote refreshes publish one
/// of these overlays by replacing a single `Arc`; readers union at most the one base vector and
/// one overlay vector for the requested series.
#[derive(Debug)]
pub(crate) struct ImmutableTombstoneShard {
    entries: TombstoneMap,
    memory_usage_bytes: usize,
}

impl ImmutableTombstoneShard {
    pub(crate) fn from_map_with_memory_usage(
        entries: TombstoneMap,
        memory_usage_bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            entries,
            memory_usage_bytes: memory_usage_bytes.saturating_add(Self::fixed_allocation_bytes()),
        })
    }

    fn fixed_allocation_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(std::mem::size_of::<usize>().saturating_mul(2))
    }

    pub(crate) fn memory_usage_bytes(&self) -> usize {
        self.memory_usage_bytes
    }
}

impl std::ops::Deref for ImmutableTombstoneShard {
    type Target = TombstoneMap;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

#[derive(Debug)]
pub(crate) struct ImmutableTombstoneSnapshot {
    shards: Box<[Arc<ImmutableTombstoneShard>]>,
    entry_count: usize,
    memory_usage_bytes: usize,
    max_series_id: Option<SeriesId>,
}

impl ImmutableTombstoneSnapshot {
    fn fixed_allocation_bytes() -> usize {
        std::mem::size_of::<Self>()
            // Arc strong/weak counters for the snapshot allocation.
            .saturating_add(std::mem::size_of::<usize>().saturating_mul(2))
            // The boxed fixed-fanout table stores one Arc pointer per shard.
            .saturating_add(
                LIVE_TOMBSTONE_SHARD_COUNT
                    .saturating_mul(std::mem::size_of::<Arc<ImmutableTombstoneShard>>()),
            )
    }

    pub(crate) fn empty_memory_usage_bytes() -> usize {
        Self::fixed_allocation_bytes().saturating_add(
            LIVE_TOMBSTONE_SHARD_COUNT
                .saturating_mul(ImmutableTombstoneShard::fixed_allocation_bytes()),
        )
    }

    pub(crate) fn empty() -> Arc<Self> {
        let shards = (0..LIVE_TOMBSTONE_SHARD_COUNT)
            .map(|_| ImmutableTombstoneShard::from_map_with_memory_usage(TombstoneMap::new(), 0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            shards,
            entry_count: 0,
            memory_usage_bytes: Self::empty_memory_usage_bytes(),
            max_series_id: None,
        })
    }

    pub(crate) fn shard_index(series_id: SeriesId) -> usize {
        usize::try_from(series_id % LIVE_TOMBSTONE_SHARD_COUNT as u64)
            .expect("fixed tombstone shard index fits usize")
    }

    pub(crate) fn from_shards(shards: Vec<Arc<ImmutableTombstoneShard>>) -> Self {
        assert_eq!(
            shards.len(),
            LIVE_TOMBSTONE_SHARD_COUNT,
            "immutable tombstone snapshots have fixed fanout"
        );
        let mut entry_count = 0usize;
        let mut memory_usage_bytes = Self::fixed_allocation_bytes();
        let mut max_series_id = None;
        for shard in &shards {
            entry_count = entry_count.saturating_add(shard.len());
            memory_usage_bytes = memory_usage_bytes.saturating_add(shard.memory_usage_bytes());
            max_series_id = max_series_id.max(shard.keys().next_back().copied());
        }
        Self {
            shards: shards.into_boxed_slice(),
            entry_count,
            memory_usage_bytes,
            max_series_id,
        }
    }

    pub(crate) fn shard(&self, shard_index: usize) -> &Arc<ImmutableTombstoneShard> {
        &self.shards[shard_index]
    }

    pub(crate) fn ranges(&self, series_id: SeriesId) -> Option<&[TombstoneRange]> {
        self.shards[Self::shard_index(series_id)]
            .get(&series_id)
            .map(Vec::as_slice)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub(crate) fn memory_usage_bytes(&self) -> usize {
        self.memory_usage_bytes
    }

    pub(crate) fn max_series_id(&self) -> Option<SeriesId> {
        self.max_series_id
    }
}

/// Conservative retained allocation charged for each `BTreeMap` entry.
///
/// `std::collections::BTreeMap` does not expose node capacity. Charging one generously sized node
/// per entry covers the least-dense legal tree plus allocator metadata without relying on private
/// standard-library constants. The deliberately stable value also makes admission boundaries
/// deterministic across allocator and compiler versions.
pub(crate) const TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneFileV1 {
    version: u16,
    entries: Vec<TombstoneSeriesEntryV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneSeriesEntryV1 {
    series_id: SeriesId,
    ranges: Vec<TombstoneRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneStoreManifestV2 {
    version: u16,
    shard_count: u16,
    shards: Vec<Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneShardFileV2 {
    version: u16,
    entries: Vec<TombstoneSeriesEntryV1>,
}

struct PreparedTombstoneShard {
    file_name: String,
    path: PathBuf,
    payload: Vec<u8>,
}

struct PreparedTombstoneStoreUpdate {
    lane: TombstoneLane,
    path: PathBuf,
    previous_bytes: Option<Vec<u8>>,
    next_manifest_payload: Option<Vec<u8>>,
    new_shards: Vec<PreparedTombstoneShard>,
    candidate_shards: Vec<TombstoneTransactionShardRecord>,
}

fn path_retained_bytes(path: &Path) -> usize {
    path.as_os_str()
        .len()
        .saturating_add(std::mem::size_of::<PathBuf>())
}

fn prepared_shard_retained_bytes(shard: &PreparedTombstoneShard) -> usize {
    std::mem::size_of::<PreparedTombstoneShard>()
        .saturating_add(shard.file_name.capacity())
        .saturating_add(path_retained_bytes(&shard.path))
        .saturating_add(shard.payload.capacity())
}

fn transaction_shard_record_retained_bytes(record: &TombstoneTransactionShardRecord) -> usize {
    std::mem::size_of::<TombstoneTransactionShardRecord>()
        .saturating_add(record.file_name.capacity())
}

fn prepared_tombstone_plan_retained_bytes(plan: &PreparedTombstoneStoreUpdate) -> usize {
    std::mem::size_of::<PreparedTombstoneStoreUpdate>()
        .saturating_add(path_retained_bytes(&plan.lane.namespace_root))
        .saturating_add(path_retained_bytes(&plan.lane.manifest_path))
        .saturating_add(path_retained_bytes(&plan.path))
        .saturating_add(plan.previous_bytes.as_ref().map_or(0, Vec::capacity))
        .saturating_add(plan.next_manifest_payload.as_ref().map_or(0, Vec::capacity))
        .saturating_add(plan.new_shards.iter().fold(0usize, |total, shard| {
            total.saturating_add(prepared_shard_retained_bytes(shard))
        }))
        .saturating_add(plan.candidate_shards.iter().fold(0usize, |total, record| {
            total.saturating_add(transaction_shard_record_retained_bytes(record))
        }))
        .saturating_add(4096)
}

fn transaction_record_from_plans_retained_upper_bound(
    plans: &[PreparedTombstoneStoreUpdate],
) -> usize {
    std::mem::size_of::<TombstoneTransactionRecord>()
        .saturating_add(plans.iter().fold(0usize, |total, plan| {
            total
                .saturating_add(std::mem::size_of::<TombstoneTransactionLaneRecord>())
                .saturating_add(path_retained_bytes(&plan.lane.namespace_root))
                .saturating_add(path_retained_bytes(&plan.lane.manifest_path))
                .saturating_add(plan.previous_bytes.as_ref().map_or(0, Vec::capacity))
                .saturating_add(plan.next_manifest_payload.as_ref().map_or(0, Vec::capacity))
                .saturating_add(plan.candidate_shards.iter().fold(
                    0usize,
                    |record_total, record| {
                        record_total.saturating_add(transaction_shard_record_retained_bytes(record))
                    },
                ))
        }))
        .saturating_add(4096)
}

fn transaction_record_retained_bytes(record: &TombstoneTransactionRecord) -> usize {
    std::mem::size_of::<TombstoneTransactionRecord>()
        .saturating_add(record.lanes.iter().fold(0usize, |total, lane| {
            total
                .saturating_add(std::mem::size_of::<TombstoneTransactionLaneRecord>())
                .saturating_add(path_retained_bytes(&lane.namespace_root_identity))
                .saturating_add(path_retained_bytes(&lane.manifest_path_identity))
                .saturating_add(lane.previous_manifest.as_ref().map_or(0, Vec::capacity))
                .saturating_add(lane.candidate_manifest.as_ref().map_or(0, Vec::capacity))
                .saturating_add(
                    lane.candidate_shards
                        .iter()
                        .fold(0usize, |record_total, shard| {
                            record_total
                                .saturating_add(transaction_shard_record_retained_bytes(shard))
                        }),
                )
        }))
        .saturating_add(4096)
}

fn path_collection_retained_upper_bound(path_count: usize, path_bytes: usize) -> usize {
    let bucket_count = path_count
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4)
        .saturating_mul(2);
    bucket_count
        .saturating_mul(std::mem::size_of::<PathBuf>().saturating_add(32))
        // PathBuf does not expose cloned allocation capacity; geometric allocator growth is
        // bounded conservatively by twice the observed byte length.
        .saturating_add(path_bytes.saturating_mul(2))
        .saturating_add(16 * 1024)
}

fn tombstone_transaction_path_staging_upper_bound(
    data_path: &Path,
    plans: &[PreparedTombstoneStoreUpdate],
) -> usize {
    let mut target_count = 1usize;
    let mut target_path_bytes = path_retained_bytes(&tombstone_transaction_path(data_path));
    for plan in plans {
        target_count = target_count
            .saturating_add(plan.new_shards.len())
            .saturating_add(usize::from(plan.next_manifest_payload.is_some()));
        target_path_bytes = target_path_bytes.saturating_add(
            plan.new_shards.iter().fold(0usize, |total, shard| {
                total.saturating_add(path_retained_bytes(&shard.path))
            }),
        );
        if plan.next_manifest_payload.is_some() {
            target_path_bytes = target_path_bytes.saturating_add(path_retained_bytes(&plan.path));
        }
    }

    // Disk preflight retains the governed target list and a missing-parent BTreeSet. Directory
    // preparation then retains a parent HashSet. They run sequentially, but this intentionally
    // covers both plus target/path allocator rounding without depending on Vec length.
    path_collection_retained_upper_bound(target_count, target_path_bytes)
        .saturating_add(target_path_bytes.saturating_mul(6))
        .saturating_add(target_count.saturating_mul(512))
        .saturating_add(64 * 1024)
}

fn tombstone_owned_shard_paths_retained_upper_bound(
    plans: &[PreparedTombstoneStoreUpdate],
) -> usize {
    let path_count = plans.iter().fold(0usize, |total, plan| {
        total.saturating_add(plan.new_shards.len())
    });
    let path_bytes = plans.iter().fold(0usize, |total, plan| {
        total.saturating_add(plan.new_shards.iter().fold(0usize, |plan_total, shard| {
            plan_total.saturating_add(path_retained_bytes(&shard.path))
        }))
    });
    path_collection_retained_upper_bound(path_count, path_bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum TombstoneLaneRole {
    LocalNumeric,
    LocalBlob,
    HotNumeric,
    HotBlob,
    WarmNumeric,
    WarmBlob,
    ColdNumeric,
    ColdBlob,
}

impl TombstoneLaneRole {
    pub(crate) fn is_shared_remote(self) -> bool {
        matches!(
            self,
            Self::HotNumeric
                | Self::HotBlob
                | Self::WarmNumeric
                | Self::WarmBlob
                | Self::ColdNumeric
                | Self::ColdBlob
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TombstoneLane {
    pub(crate) role: TombstoneLaneRole,
    /// Configured root whose own aliasing is allowed; every derived component below it is not.
    pub(crate) namespace_root: PathBuf,
    pub(crate) manifest_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RemoteTombstoneManifestFingerprint {
    pub(crate) exists: bool,
    pub(crate) logical_bytes: u64,
    pub(crate) xxh64: u64,
}

#[derive(Debug)]
pub(crate) enum RemoteTombstoneManifestKind {
    Missing,
    Legacy,
    Sharded(Vec<Option<String>>),
}

#[derive(Debug)]
pub(crate) struct RemoteTombstoneManifestSnapshot {
    pub(crate) fingerprint: RemoteTombstoneManifestFingerprint,
    pub(crate) kind: RemoteTombstoneManifestKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TombstoneTransactionPhase {
    Prepared,
    Committing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TombstoneTransactionLaneRecord {
    role: TombstoneLaneRole,
    namespace_root_identity: PathBuf,
    /// Identity-only copy of the configured target. Recovery never uses this as an I/O path.
    manifest_path_identity: PathBuf,
    previous_manifest: Option<Vec<u8>>,
    candidate_manifest: Option<Vec<u8>>,
    candidate_shards: Vec<TombstoneTransactionShardRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TombstoneTransactionShardRecord {
    shard_index: u16,
    file_name: String,
    logical_bytes: u64,
    xxh64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TombstoneTransactionRecord {
    version: u16,
    transaction_id: u64,
    phase: TombstoneTransactionPhase,
    lanes: Vec<TombstoneTransactionLaneRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TombstoneRecoveryOutcome {
    NoTransaction,
    RolledBackPrepared,
    RolledForwardCommitted,
}

pub(crate) struct PreparedCommittedTombstoneReload {
    pub(crate) tombstones: TombstoneMap,
    pub(crate) recovery_memory_upper_bound: usize,
    pub(crate) work_items: usize,
    pub(crate) work_bytes: u64,
}

impl TombstoneRecoveryOutcome {
    pub(crate) fn requires_authoritative_reload(self) -> bool {
        self == Self::RolledForwardCommitted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TombstonePersistenceCertainty {
    DefinitivelyClean,
    Indeterminate,
    Committed,
}

#[derive(Debug)]
pub(crate) struct TombstonePersistenceError {
    error: TsinkError,
    certainty: TombstonePersistenceCertainty,
}

pub(crate) type TombstonePersistenceResult<T> = std::result::Result<T, TombstonePersistenceError>;

impl TombstonePersistenceError {
    pub(crate) fn definitively_clean(error: TsinkError) -> Self {
        Self {
            error,
            certainty: TombstonePersistenceCertainty::DefinitivelyClean,
        }
    }

    pub(crate) fn indeterminate(error: TsinkError) -> Self {
        Self {
            error,
            certainty: TombstonePersistenceCertainty::Indeterminate,
        }
    }

    pub(crate) fn committed(error: TsinkError) -> Self {
        Self {
            error,
            certainty: TombstonePersistenceCertainty::Committed,
        }
    }

    pub(crate) fn is_definitively_clean(&self) -> bool {
        self.certainty == TombstonePersistenceCertainty::DefinitivelyClean
    }

    pub(crate) fn is_committed(&self) -> bool {
        self.certainty == TombstonePersistenceCertainty::Committed
    }

    pub(crate) fn into_tsink_error(self) -> TsinkError {
        self.error
    }
}

impl From<TsinkError> for TombstonePersistenceError {
    fn from(error: TsinkError) -> Self {
        Self::definitively_clean(error)
    }
}

fn tombstone_transaction_dir(data_path: &Path) -> PathBuf {
    data_path.join(TOMBSTONE_TRANSACTION_DIR_NAME)
}

fn tombstone_transaction_path(data_path: &Path) -> PathBuf {
    tombstone_transaction_dir(data_path).join(TOMBSTONE_TRANSACTION_FILE_NAME)
}

fn absolute_path_lexically_normalized(path: &Path) -> Result<PathBuf> {
    crate::engine::fs_utils::absolute_path_lexically_normalized(path)
}

fn resolve_trusted_namespace_root(path: &Path) -> Result<PathBuf> {
    crate::engine::fs_utils::resolve_trusted_namespace_root(path)
}

pub(crate) fn normalize_tombstone_lanes(lanes: &[TombstoneLane]) -> Result<Vec<TombstoneLane>> {
    lanes
        .iter()
        .map(|lane| {
            let lexical_root = absolute_path_lexically_normalized(&lane.namespace_root)?;
            let lexical_manifest = absolute_path_lexically_normalized(&lane.manifest_path)?;
            let relative = lexical_manifest.strip_prefix(&lexical_root).map_err(|_| {
                TsinkError::InvalidConfiguration(format!(
                    "tombstone manifest {} is outside configured namespace root {}",
                    lane.manifest_path.display(),
                    lane.namespace_root.display()
                ))
            })?;
            let namespace_root = resolve_trusted_namespace_root(&lexical_root)?;
            Ok(TombstoneLane {
                role: lane.role,
                manifest_path: namespace_root.join(relative),
                namespace_root,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum OwnedTombstoneEntryKind {
    File,
    Directory,
}

fn validate_boundary_directory(
    root: &Path,
    description: &str,
    allow_root_alias: bool,
) -> Result<()> {
    crate::engine::fs_utils::validate_boundary_directory(root, description, allow_root_alias)
}

fn validate_owned_entry_below_alias_boundary(
    root: &Path,
    target: &Path,
    final_kind: OwnedTombstoneEntryKind,
) -> Result<()> {
    let final_kind = match final_kind {
        OwnedTombstoneEntryKind::File => crate::engine::fs_utils::OwnedBoundaryEntryKind::File,
        OwnedTombstoneEntryKind::Directory => {
            crate::engine::fs_utils::OwnedBoundaryEntryKind::Directory
        }
    };
    crate::engine::fs_utils::validate_owned_entry_below_alias_boundary(
        root,
        target,
        final_kind,
        "owned tombstone",
    )
}

fn validate_tombstone_lane_namespace(lane: &TombstoneLane) -> Result<()> {
    let lane_root = lane.manifest_path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "tombstone lane manifest has no lane root: {}",
            lane.manifest_path.display()
        ))
    })?;
    validate_boundary_directory(
        &lane.namespace_root,
        "configured tombstone namespace root",
        true,
    )?;
    if lane_root != lane.namespace_root {
        validate_owned_entry_below_alias_boundary(
            &lane.namespace_root,
            lane_root,
            OwnedTombstoneEntryKind::Directory,
        )?;
    }
    validate_owned_entry_below_alias_boundary(
        &lane.namespace_root,
        &lane.manifest_path,
        OwnedTombstoneEntryKind::File,
    )?;
    validate_owned_entry_below_alias_boundary(
        &lane.namespace_root,
        &tombstone_store_dir(&lane.manifest_path),
        OwnedTombstoneEntryKind::Directory,
    )?;
    validate_owned_entry_below_alias_boundary(
        &lane.namespace_root,
        &tombstone_shards_dir(&lane.manifest_path),
        OwnedTombstoneEntryKind::Directory,
    )
}

fn validate_tombstone_shard_path(path: &Path) -> Result<()> {
    let shards_dir = path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "tombstone shard has no shards directory: {}",
            path.display()
        ))
    })?;
    if shards_dir.file_name() != Some(std::ffi::OsStr::new(TOMBSTONE_SHARDS_DIR_NAME)) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "tombstone shard is outside the exact shards namespace: {}",
            path.display()
        )));
    }
    let store_dir = shards_dir.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "tombstone shard has no store directory: {}",
            path.display()
        ))
    })?;
    let lane_root = store_dir.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "tombstone shard has no lane root: {}",
            path.display()
        ))
    })?;
    validate_boundary_directory(lane_root, "configured tombstone lane root", false)?;
    validate_owned_entry_below_alias_boundary(
        lane_root,
        store_dir,
        OwnedTombstoneEntryKind::Directory,
    )?;
    validate_owned_entry_below_alias_boundary(
        lane_root,
        shards_dir,
        OwnedTombstoneEntryKind::Directory,
    )?;
    validate_owned_entry_below_alias_boundary(lane_root, path, OwnedTombstoneEntryKind::File)
}

fn validate_tombstone_coordinator_namespace(data_path: &Path) -> Result<()> {
    validate_boundary_directory(data_path, "configured tombstone data root", true)?;
    validate_owned_entry_below_alias_boundary(
        data_path,
        &tombstone_transaction_dir(data_path),
        OwnedTombstoneEntryKind::Directory,
    )?;
    validate_owned_entry_below_alias_boundary(
        data_path,
        &tombstone_transaction_path(data_path),
        OwnedTombstoneEntryKind::File,
    )
}

fn is_exact_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn atomic_write_temp_target_name(file_name: &str) -> Option<&str> {
    let generated = file_name.strip_prefix('.')?;
    let (target, suffix) = generated.rsplit_once(".tmp-")?;
    if target.is_empty() {
        return None;
    }
    let (pid, nonce) = suffix.split_once('-')?;
    let canonical_pid = pid
        .parse::<u32>()
        .ok()
        .is_some_and(|value| value.to_string() == pid);
    (canonical_pid && is_exact_lower_hex(nonce, 16)).then_some(target)
}

fn collect_unmanaged_atomic_write_temps_with_memory_admission(
    directory: &Path,
    owns_target: impl Fn(&str) -> bool,
    remaining_entries: &mut usize,
    owned_paths: &mut Vec<PathBuf>,
    owned_path_payload_bytes: &mut usize,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: directory.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_dir()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
    {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone atomic-temp namespace must be a directory and may not be a symlink: {}",
            directory.display()
        )));
    }

    let entry_transient = tombstone_cleanup_entry_transient_bytes(directory);
    let retained = tombstone_cleanup_orphan_paths_retained_bytes(
        owned_paths.capacity(),
        *owned_path_payload_bytes,
    );
    admit_memory(retained.saturating_add(entry_transient))?;
    let mut entries = std::fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    loop {
        let retained = tombstone_cleanup_orphan_paths_retained_bytes(
            owned_paths.capacity(),
            *owned_path_payload_bytes,
        );
        admit_memory(retained.saturating_add(entry_transient))?;
        let Some(entry) = entries.next() else {
            break;
        };
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        if *remaining_entries == 0 {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone atomic-temp cleanup exceeds the global {}-entry recovery bound",
                crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES
            )));
        }
        *remaining_entries -= 1;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(target_name) = atomic_write_temp_target_name(name) else {
            continue;
        };
        if !owns_target(target_name) {
            continue;
        }
        let path = entry.path();
        let path_len = path.as_os_str().len();
        if path_len > TOMBSTONE_CLEANUP_ENTRY_PATH_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "owned tombstone atomic temporary path exceeds the {}-byte recovery bound: {}",
                TOMBSTONE_CLEANUP_ENTRY_PATH_BYTES,
                path.display()
            )));
        }
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })?;
        if !metadata.file_type().is_file()
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        {
            return Err(TsinkError::DataCorruption(format!(
                "owned tombstone atomic temporary must be a regular file and may not be a symlink: {}",
                path.display()
            )));
        }
        let next_path_payload_bytes = owned_path_payload_bytes.saturating_add(path_len);
        let required_len = owned_paths.len().saturating_add(1);
        let next_vector_capacity = if required_len <= owned_paths.capacity() {
            owned_paths.capacity()
        } else {
            required_len.next_power_of_two().max(4)
        };
        admit_memory(
            tombstone_cleanup_orphan_paths_retained_bytes(
                next_vector_capacity,
                next_path_payload_bytes,
            )
            .saturating_add(entry_transient),
        )?;
        owned_paths.push(path);
        *owned_path_payload_bytes = next_path_payload_bytes;
    }
    Ok(())
}

fn cleanup_tombstone_atomic_temps_before_recovery(
    data_path: &Path,
    lanes: &[TombstoneLane],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let mut remaining_entries = crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES;
    let mut owned_paths = Vec::new();
    let mut owned_path_payload_bytes = 0usize;
    collect_unmanaged_atomic_write_temps_with_memory_admission(
        &tombstone_transaction_dir(data_path),
        |target| target == TOMBSTONE_TRANSACTION_FILE_NAME,
        &mut remaining_entries,
        &mut owned_paths,
        &mut owned_path_payload_bytes,
        &mut admit_memory,
    )?;

    for lane in lanes {
        collect_unmanaged_atomic_write_temps_with_memory_admission(
            lane.manifest_path.parent().ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "tombstone manifest has no parent: {}",
                    lane.manifest_path.display()
                ))
            })?,
            |target| target == TOMBSTONES_FILE_NAME,
            &mut remaining_entries,
            &mut owned_paths,
            &mut owned_path_payload_bytes,
            &mut admit_memory,
        )?;
        collect_unmanaged_atomic_write_temps_with_memory_admission(
            &tombstone_shards_dir(&lane.manifest_path),
            is_owned_tombstone_shard_name,
            &mut remaining_entries,
            &mut owned_paths,
            &mut owned_path_payload_bytes,
            &mut admit_memory,
        )?;
    }

    // Enumeration and every memory check complete before the first deletion. Exact-file removal
    // preserves the no-follow boundary. One lazy Recovery reservation covers every governed
    // temporary and exact accounting is installed by at most one terminal reconciliation.
    remove_owned_regular_files_and_sync_parents_budgeted(
        owned_paths.iter().map(PathBuf::as_path),
        budget,
        crate::DiskCategory::Temporary,
        |_| Ok(()),
    )
    .map(|_| ())
}

fn preflight_recovery_namespace_directory(
    directory: &Path,
    item_limit: usize,
    byte_limit: u64,
    selected_items: &mut usize,
    selected_bytes: &mut u64,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: directory.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_dir()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
    {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone recovery namespace must be a directory and may not be link-like: {}",
            directory.display()
        )));
    }
    // The read-only preflight and the subsequent cleanup each open and enumerate this directory.
    *selected_items = selected_items.saturating_add(2);
    *selected_bytes = selected_bytes.saturating_add(8 * 1024);
    if *selected_items > item_limit {
        return Err(TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "committed tombstone recovery namespace",
            item_limit,
            byte_limit,
            selected_items: *selected_items,
            selected_bytes: *selected_bytes,
        });
    }
    if *selected_bytes > byte_limit {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "committed tombstone recovery namespace",
            limit: byte_limit,
            required: *selected_bytes,
        });
    }
    let entries = std::fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        let path_bytes = entry.path().as_os_str().as_encoded_bytes().len();
        let entry_work_bytes = u64::try_from(path_bytes)
            .unwrap_or(u64::MAX)
            .saturating_mul(2)
            .saturating_add(8 * 1024);
        *selected_items = selected_items.saturating_add(2);
        *selected_bytes = selected_bytes.saturating_add(entry_work_bytes);
        if *selected_items > item_limit {
            return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                operation: "committed tombstone recovery namespace",
                item_limit,
                byte_limit,
                selected_items: *selected_items,
                selected_bytes: *selected_bytes,
            });
        }
        if *selected_bytes > byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: "committed tombstone recovery namespace",
                limit: byte_limit,
                required: *selected_bytes,
            });
        }
    }
    Ok(())
}

/// Read-only dependency-window preflight for the namespace enumeration that recovery repeats
/// before its first temporary-file deletion or manifest mutation.
pub(crate) fn preflight_tombstone_recovery_namespace_work(
    data_path: &Path,
    lanes: &[TombstoneLane],
    item_limit: usize,
    byte_limit: u64,
    base_items: usize,
    base_bytes: u64,
) -> Result<(usize, u64)> {
    let mut selected_items = base_items;
    let mut selected_bytes = base_bytes;
    preflight_recovery_namespace_directory(
        &tombstone_transaction_dir(data_path),
        item_limit,
        byte_limit,
        &mut selected_items,
        &mut selected_bytes,
    )?;
    for lane in lanes {
        let manifest_parent = lane.manifest_path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "tombstone manifest has no parent: {}",
                lane.manifest_path.display()
            ))
        })?;
        preflight_recovery_namespace_directory(
            manifest_parent,
            item_limit,
            byte_limit,
            &mut selected_items,
            &mut selected_bytes,
        )?;
        let shards_dir = tombstone_shards_dir(&lane.manifest_path);
        preflight_recovery_namespace_directory(
            &shards_dir,
            item_limit,
            byte_limit,
            &mut selected_items,
            &mut selected_bytes,
        )?;
    }
    Ok((selected_items, selected_bytes))
}

pub(crate) fn validate_tombstone_lanes(lanes: &[TombstoneLane]) -> Result<()> {
    if lanes.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "a tombstone transaction requires at least one configured lane".to_string(),
        ));
    }
    let mut roles = HashSet::new();
    let mut paths = HashSet::new();
    let mut previous_role = None;
    for lane in lanes {
        if previous_role.is_some_and(|role| role >= lane.role) {
            return Err(TsinkError::InvalidConfiguration(
                "tombstone transaction lanes must use unique roles in stable order".to_string(),
            ));
        }
        previous_role = Some(lane.role);
        if !roles.insert(lane.role) || !paths.insert(lane.manifest_path.clone()) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "duplicate tombstone transaction lane {:?} at {}",
                lane.role,
                lane.manifest_path.display()
            )));
        }
        if lane.manifest_path.file_name() != Some(std::ffi::OsStr::new(TOMBSTONES_FILE_NAME)) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "tombstone lane manifest must end in {TOMBSTONES_FILE_NAME}: {}",
                lane.manifest_path.display()
            )));
        }
        validate_tombstone_lane_namespace(lane)?;
    }
    Ok(())
}

fn tombstone_regular_file_read_options() -> std::fs::OpenOptions {
    let mut options = std::fs::OpenOptions::new();
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

fn read_optional_regular_file_bounded(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
    {
        return Err(TsinkError::DataCorruption(format!(
            "owned tombstone state must be a regular file and may not be a symlink: {}",
            path.display()
        )));
    }
    let file_len = usize::try_from(metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "owned tombstone state is too large to read: {}",
            path.display()
        ))
    })?;
    if file_len > limit {
        return Err(TsinkError::DataCorruption(format!(
            "owned tombstone state exceeds its {limit}-byte bound: {}",
            path.display()
        )));
    }
    let options = tombstone_regular_file_read_options();
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
    if crate::engine::fs_utils::is_link_or_reparse_point(&opened_metadata)
        || !opened_metadata.file_type().is_file()
        || opened_metadata.len() != metadata.len()
    {
        return Err(TsinkError::DataCorruption(format!(
            "owned tombstone state changed type or length while opening: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(file_len);
    (&mut file)
        .take(u64::try_from(file_len).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let mut growth_probe = [0u8; 1];
    let grew = file
        .read(&mut growth_probe)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?
        != 0;
    if bytes.len() != file_len || grew {
        return Err(TsinkError::DataCorruption(format!(
            "owned tombstone state changed length while reading within its {limit}-byte bound: {}",
            path.display()
        )));
    }
    Ok(Some(bytes))
}

fn read_required_regular_file_bounded_exact(
    path: &Path,
    limit: usize,
    expected_len: usize,
) -> Result<Vec<u8>> {
    read_required_regular_file_bounded_exact_with_before_open(path, limit, expected_len, || {})
}

fn read_required_regular_file_bounded_exact_with_before_open(
    path: &Path,
    limit: usize,
    expected_len: usize,
    before_open: impl FnOnce(),
) -> Result<Vec<u8>> {
    if expected_len > limit {
        return Err(TsinkError::DataCorruption(format!(
            "required tombstone state expected length {expected_len} exceeds its {limit}-byte bound: {}",
            path.display()
        )));
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            TsinkError::DataCorruption(format!(
                "required tombstone state is missing: {}",
                path.display()
            ))
        } else {
            TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            }
        }
    })?;
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || metadata.len() != expected_len as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "required tombstone state changed type or length after memory admission: {}",
            path.display()
        )));
    }
    before_open();
    let options = tombstone_regular_file_read_options();
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
    if crate::engine::fs_utils::is_link_or_reparse_point(&opened_metadata)
        || !opened_metadata.file_type().is_file()
        || opened_metadata.len() != expected_len as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "required tombstone state changed type or length while opening: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(expected_len);
    (&mut file)
        .take(u64::try_from(expected_len).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let mut growth_probe = [0u8; 1];
    let grew = file
        .read(&mut growth_probe)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?
        != 0;
    if bytes.len() != expected_len || grew {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone state length changed after memory admission: expected {expected_len}, found {} at {}",
            bytes.len(),
            path.display()
        )));
    }
    Ok(bytes)
}

fn read_optional_regular_file_bounded_exact_with_admission(
    path: &Path,
    limit: usize,
    mut admit_length: impl FnMut(usize) -> Result<()>,
) -> Result<Option<Vec<u8>>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || metadata.len() > limit as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "owned tombstone state has invalid type or length: {}",
            path.display()
        )));
    }
    let expected_len = usize::try_from(metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "owned tombstone state length is unsupported: {}",
            path.display()
        ))
    })?;
    admit_length(expected_len)?;
    read_required_regular_file_bounded_exact(path, limit, expected_len).map(Some)
}

fn tombstone_transaction_bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES as u64)
        .reject_trailing_bytes()
}

fn tombstone_transaction_encoded_len(record: &TombstoneTransactionRecord) -> Result<usize> {
    let payload_len = usize::try_from(
        tombstone_transaction_bincode_options().serialized_size(record)?,
    )
    .map_err(|_| {
        TsinkError::InvalidConfiguration(
            "tombstone transaction coordinator length exceeds the supported platform range"
                .to_string(),
        )
    })?;
    let total_len = TOMBSTONE_TRANSACTION_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(TOMBSTONE_TRANSACTION_CHECKSUM_LEN))
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "tombstone transaction coordinator length overflow".to_string(),
            )
        })?;
    if total_len > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "tombstone transaction coordinator requires {total_len} bytes, exceeding the {}-byte bound",
            MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES
        )));
    }
    Ok(total_len)
}

fn encode_tombstone_transaction_record_with_memory_admission(
    record: &TombstoneTransactionRecord,
    retained_bytes: usize,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<Vec<u8>> {
    let expected_encoded_len = tombstone_transaction_encoded_len(record)?;
    // Bincode first allocates its payload Vec. Framing then allocates a second Vec while that
    // payload is still live. Reserve both from the measured serialized size before either
    // allocation; after serialization, reconcile allocator rounding before allocating the frame.
    admit_memory(
        retained_bytes
            .saturating_add(expected_encoded_len.saturating_mul(2))
            .saturating_add(TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK),
    )?;
    let payload = tombstone_transaction_bincode_options().serialize(record)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "tombstone transaction coordinator payload exceeds the supported range".to_string(),
        )
    })?;
    let total_len = TOMBSTONE_TRANSACTION_HEADER_LEN
        .checked_add(payload.len())
        .and_then(|len| len.checked_add(TOMBSTONE_TRANSACTION_CHECKSUM_LEN))
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "tombstone transaction coordinator length overflow".to_string(),
            )
        })?;
    if total_len > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "tombstone transaction coordinator requires {total_len} bytes, exceeding the {}-byte bound",
            MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES
        )));
    }
    admit_memory(
        retained_bytes
            .saturating_add(payload.capacity())
            .saturating_add(total_len)
            .saturating_add(TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK),
    )?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(&TOMBSTONE_TRANSACTION_MAGIC);
    append_u16(&mut bytes, TOMBSTONE_TRANSACTION_VERSION);
    append_u32(&mut bytes, payload_len);
    bytes.extend_from_slice(&payload);
    let checksum = checksum32(&bytes);
    append_u32(&mut bytes, checksum);
    Ok(bytes)
}

#[cfg(test)]
fn encode_tombstone_transaction_record(record: &TombstoneTransactionRecord) -> Result<Vec<u8>> {
    encode_tombstone_transaction_record_with_memory_admission(record, 0, |_| Ok(()))
}

fn preflight_tombstone_transaction_payload(payload: &[u8]) -> Result<()> {
    fn read_bounded_blob<'a>(
        payload: &'a [u8],
        position: &mut usize,
        max_len: usize,
        description: &str,
    ) -> Result<&'a [u8]> {
        let len = usize::try_from(read_bincode_u64(payload, position, description)?)
            .map_err(|_| TsinkError::DataCorruption(format!("unsupported {description} length")))?;
        if len > max_len {
            return Err(TsinkError::DataCorruption(format!(
                "{description} exceeds its {max_len}-byte bound"
            )));
        }
        take_bincode_bytes(payload, position, len, description)
    }

    fn read_manifest_option(payload: &[u8], position: &mut usize) -> Result<()> {
        match read_bincode_u8(payload, position, "tombstone manifest option tag")? {
            0 => Ok(()),
            1 => read_bounded_blob(
                payload,
                position,
                MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
                "tombstone manifest image",
            )
            .map(|_| ()),
            tag => Err(TsinkError::DataCorruption(format!(
                "invalid tombstone manifest option tag {tag}"
            ))),
        }
    }

    let mut position = 0usize;
    let version = read_bincode_u16(payload, &mut position, "transaction payload version")?;
    if version != TOMBSTONE_TRANSACTION_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone transaction payload version {version}"
        )));
    }
    let transaction_id = read_bincode_u64(payload, &mut position, "transaction id")?;
    if transaction_id == 0 {
        return Err(TsinkError::DataCorruption(
            "tombstone transaction coordinator has a zero transaction id".to_string(),
        ));
    }
    let phase = read_bincode_u32(payload, &mut position, "transaction phase")?;
    if phase > 1 {
        return Err(TsinkError::DataCorruption(format!(
            "invalid tombstone transaction phase {phase}"
        )));
    }
    let lane_count = usize::try_from(read_bincode_u64(
        payload,
        &mut position,
        "transaction lane count",
    )?)
    .map_err(|_| TsinkError::DataCorruption("unsupported transaction lane count".to_string()))?;
    if !(1..=8).contains(&lane_count) {
        return Err(TsinkError::DataCorruption(format!(
            "invalid tombstone transaction lane count {lane_count}"
        )));
    }
    for _ in 0..lane_count {
        let role = read_bincode_u32(payload, &mut position, "tombstone lane role")?;
        if role > 7 {
            return Err(TsinkError::DataCorruption(format!(
                "invalid tombstone lane role {role}"
            )));
        }
        for description in ["tombstone namespace path", "tombstone manifest path"] {
            let path = read_bounded_blob(
                payload,
                &mut position,
                MAX_TOMBSTONE_TRANSACTION_PATH_BYTES,
                description,
            )?;
            std::str::from_utf8(path).map_err(|_| {
                TsinkError::DataCorruption(format!("{description} is not valid UTF-8"))
            })?;
        }
        read_manifest_option(payload, &mut position)?;
        read_manifest_option(payload, &mut position)?;
        let shard_count = usize::try_from(read_bincode_u64(
            payload,
            &mut position,
            "candidate shard count",
        )?)
        .map_err(|_| TsinkError::DataCorruption("unsupported candidate shard count".to_string()))?;
        if shard_count > TOMBSTONE_STORE_SHARD_COUNT {
            return Err(TsinkError::DataCorruption(format!(
                "candidate shard count {shard_count} exceeds {TOMBSTONE_STORE_SHARD_COUNT}"
            )));
        }
        for _ in 0..shard_count {
            let shard_index = usize::from(read_bincode_u16(
                payload,
                &mut position,
                "candidate shard index",
            )?);
            let file_name = read_bounded_blob(
                payload,
                &mut position,
                "shard-000-0000000000000000.bin".len(),
                "candidate shard file name",
            )?;
            let file_name = std::str::from_utf8(file_name).map_err(|_| {
                TsinkError::DataCorruption(
                    "candidate shard file name is not valid UTF-8".to_string(),
                )
            })?;
            validate_tombstone_shard_file_name(shard_index, file_name)?;
            let logical_bytes =
                read_bincode_u64(payload, &mut position, "candidate shard logical bytes")?;
            if logical_bytes > MAX_TOMBSTONE_SHARD_BYTES as u64 {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate shard logical bytes exceed {MAX_TOMBSTONE_SHARD_BYTES}"
                )));
            }
            let _ = read_bincode_u64(payload, &mut position, "candidate shard digest")?;
        }
    }
    if position != payload.len() {
        return Err(TsinkError::DataCorruption(
            "trailing bytes in tombstone transaction payload".to_string(),
        ));
    }
    Ok(())
}

fn decode_tombstone_transaction_record(bytes: &[u8]) -> Result<TombstoneTransactionRecord> {
    if bytes.len() > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone transaction coordinator exceeds the {}-byte bound",
            MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES
        )));
    }
    if bytes.len() < TOMBSTONE_TRANSACTION_HEADER_LEN + TOMBSTONE_TRANSACTION_CHECKSUM_LEN {
        return Err(TsinkError::DataCorruption(
            "tombstone transaction coordinator is truncated".to_string(),
        ));
    }
    if bytes[..TOMBSTONE_TRANSACTION_MAGIC.len()] != TOMBSTONE_TRANSACTION_MAGIC {
        return Err(TsinkError::DataCorruption(
            "invalid tombstone transaction coordinator header".to_string(),
        ));
    }
    let mut position = TOMBSTONE_TRANSACTION_MAGIC.len();
    let version = read_u16(bytes, &mut position)?;
    if version != TOMBSTONE_TRANSACTION_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone transaction coordinator version {version}"
        )));
    }
    let payload_len = usize::try_from(read_u32(bytes, &mut position)?).map_err(|_| {
        TsinkError::DataCorruption(
            "tombstone transaction coordinator payload length is unsupported".to_string(),
        )
    })?;
    let expected_len = TOMBSTONE_TRANSACTION_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(TOMBSTONE_TRANSACTION_CHECKSUM_LEN))
        .ok_or_else(|| {
            TsinkError::DataCorruption(
                "tombstone transaction coordinator length overflow".to_string(),
            )
        })?;
    if bytes.len() != expected_len {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone transaction coordinator length mismatch: expected {expected_len}, found {}",
            bytes.len()
        )));
    }
    let checksum_offset = expected_len - TOMBSTONE_TRANSACTION_CHECKSUM_LEN;
    let mut checksum_position = checksum_offset;
    let expected_checksum = read_u32(bytes, &mut checksum_position)?;
    if checksum32(&bytes[..checksum_offset]) != expected_checksum {
        return Err(TsinkError::DataCorruption(
            "tombstone transaction coordinator checksum mismatch".to_string(),
        ));
    }
    let payload = &bytes[TOMBSTONE_TRANSACTION_HEADER_LEN..checksum_offset];
    preflight_tombstone_transaction_payload(payload)?;
    let record: TombstoneTransactionRecord =
        tombstone_transaction_bincode_options().deserialize(payload)?;
    if record.version != TOMBSTONE_TRANSACTION_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone transaction payload version {}",
            record.version
        )));
    }
    if record.lanes.is_empty() || record.lanes.len() > 8 {
        return Err(TsinkError::DataCorruption(format!(
            "invalid tombstone transaction lane count {}",
            record.lanes.len()
        )));
    }
    Ok(record)
}

pub(crate) fn merge_tombstone_range(ranges: &mut Vec<TombstoneRange>, new_range: TombstoneRange) {
    ranges.push(new_range);
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<TombstoneRange> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        if let Some(last) = merged.last_mut() {
            if last.end >= range.start {
                if range.end > last.end {
                    last.end = range.end;
                }
                continue;
            }
        }
        merged.push(range);
    }
    *ranges = merged;
}

/// Unions two normalized tombstone-range vectors in linear time.
///
/// Durable shard and legacy-manifest decoders normalize every per-series vector before it reaches
/// runtime publication. Keeping this operation batched avoids repeatedly sorting and reallocating a
/// growing destination when a bounded remote refresh adopts a large shard.
pub(crate) fn merge_normalized_tombstone_ranges(
    ranges: &mut Vec<TombstoneRange>,
    additional: &[TombstoneRange],
) -> usize {
    if additional.is_empty() {
        return 0;
    }
    if ranges.is_empty() {
        ranges.extend_from_slice(additional);
        return additional.len();
    }

    debug_assert!(ranges
        .windows(2)
        .all(|pair| pair[0].start < pair[0].end && pair[0].end < pair[1].start));
    debug_assert!(additional
        .windows(2)
        .all(|pair| pair[0].start < pair[0].end && pair[0].end < pair[1].start));

    let retained = std::mem::take(ranges);
    let mut left = retained.into_iter().peekable();
    let mut right = additional.iter().copied().peekable();
    let mut merged = Vec::<TombstoneRange>::with_capacity(left.len().saturating_add(right.len()));
    let mut consumed = 0usize;

    while left.peek().is_some() || right.peek().is_some() {
        let next = match (left.peek(), right.peek()) {
            (Some(left_range), Some(right_range))
                if (left_range.start, left_range.end) <= (right_range.start, right_range.end) =>
            {
                left.next().expect("peeked left tombstone range")
            }
            (Some(_), Some(_)) | (None, Some(_)) => {
                right.next().expect("peeked right tombstone range")
            }
            (Some(_), None) => left.next().expect("peeked left tombstone range"),
            (None, None) => unreachable!("loop requires at least one remaining range"),
        };
        consumed = consumed.saturating_add(1);
        if let Some(last) = merged.last_mut() {
            if last.end >= next.start {
                last.end = last.end.max(next.end);
                continue;
            }
        }
        merged.push(next);
    }

    *ranges = merged;
    consumed
}

/// Returns the normalized union of two borrowed vectors with exactly one destination allocation.
pub(crate) fn union_normalized_tombstone_ranges(
    left: &[TombstoneRange],
    right: &[TombstoneRange],
) -> Vec<TombstoneRange> {
    debug_assert!(left
        .windows(2)
        .all(|pair| pair[0].start < pair[0].end && pair[0].end < pair[1].start));
    debug_assert!(right
        .windows(2)
        .all(|pair| pair[0].start < pair[0].end && pair[0].end < pair[1].start));

    let mut left = left.iter().copied().peekable();
    let mut right = right.iter().copied().peekable();
    let mut merged = Vec::<TombstoneRange>::with_capacity(left.len().saturating_add(right.len()));
    while left.peek().is_some() || right.peek().is_some() {
        let next = match (left.peek(), right.peek()) {
            (Some(left_range), Some(right_range))
                if (left_range.start, left_range.end) <= (right_range.start, right_range.end) =>
            {
                left.next().expect("peeked left tombstone range")
            }
            (Some(_), Some(_)) | (None, Some(_)) => {
                right.next().expect("peeked right tombstone range")
            }
            (Some(_), None) => left.next().expect("peeked left tombstone range"),
            (None, None) => unreachable!("loop requires at least one remaining range"),
        };
        if let Some(last) = merged.last_mut() {
            if last.end >= next.start {
                last.end = last.end.max(next.end);
                continue;
            }
        }
        merged.push(next);
    }
    merged
}

/// Owned counterpart used while a finite refresh drains its private decoded fragments.
///
/// Taking ownership avoids retaining or cloning the decoded per-series vector while the new
/// immutable shard is assembled. The destination still allocates one admitted linear merge
/// buffer, and both inputs are consumed into it.
pub(crate) fn merge_normalized_tombstone_ranges_owned(
    ranges: &mut Vec<TombstoneRange>,
    additional: Vec<TombstoneRange>,
) -> usize {
    if additional.is_empty() {
        return 0;
    }
    if ranges.is_empty() {
        let consumed = additional.len();
        *ranges = additional;
        return consumed;
    }

    debug_assert!(ranges
        .windows(2)
        .all(|pair| pair[0].start < pair[0].end && pair[0].end < pair[1].start));
    debug_assert!(additional
        .windows(2)
        .all(|pair| pair[0].start < pair[0].end && pair[0].end < pair[1].start));

    let retained = std::mem::take(ranges);
    let consumed = retained.len().saturating_add(additional.len());
    let mut left = retained.into_iter().peekable();
    let mut right = additional.into_iter().peekable();
    let mut merged = Vec::<TombstoneRange>::with_capacity(consumed);

    while left.peek().is_some() || right.peek().is_some() {
        let next = match (left.peek(), right.peek()) {
            (Some(left_range), Some(right_range))
                if (left_range.start, left_range.end) <= (right_range.start, right_range.end) =>
            {
                left.next().expect("peeked left tombstone range")
            }
            (Some(_), Some(_)) | (None, Some(_)) => {
                right.next().expect("peeked right tombstone range")
            }
            (Some(_), None) => left.next().expect("peeked left tombstone range"),
            (None, None) => unreachable!("loop requires at least one remaining range"),
        };
        if let Some(last) = merged.last_mut() {
            if last.end >= next.start {
                last.end = last.end.max(next.end);
                continue;
            }
        }
        merged.push(next);
    }
    *ranges = merged;
    consumed
}

pub(crate) fn timestamp_is_tombstoned(timestamp: i64, ranges: &[TombstoneRange]) -> bool {
    let idx = ranges.partition_point(|range| range.start <= timestamp);
    if idx == 0 {
        return false;
    }
    timestamp < ranges[idx - 1].end
}

pub(crate) fn interval_fully_tombstoned(
    start_inclusive: i64,
    end_inclusive: i64,
    ranges: &[TombstoneRange],
) -> bool {
    if start_inclusive > end_inclusive {
        return false;
    }

    let idx = ranges.partition_point(|range| range.start <= start_inclusive);
    if idx == 0 {
        return false;
    }

    ranges[idx - 1].end > end_inclusive
}

pub(crate) fn exclusive_interval_fully_tombstoned(
    start: i64,
    end: i64,
    ranges: &[TombstoneRange],
) -> bool {
    start < end && interval_fully_tombstoned(start, end.saturating_sub(1), ranges)
}

fn normalize_tombstone_map(map: &mut TombstoneMap) -> Result<()> {
    for ranges in map.values_mut() {
        *ranges = normalize_tombstone_ranges(ranges.iter().copied())?;
    }
    map.retain(|_, ranges| !ranges.is_empty());
    Ok(())
}

fn normalize_tombstone_ranges(
    ranges: impl IntoIterator<Item = TombstoneRange>,
) -> Result<Vec<TombstoneRange>> {
    let mut ordered = Vec::new();
    for range in ranges {
        if range.start >= range.end {
            return Err(TsinkError::DataCorruption(
                "invalid tombstone range: start must be strictly less than end".to_string(),
            ));
        }
        ordered.push(range);
    }
    ordered.sort_by_key(|range| (range.start, range.end));

    let mut normalized = Vec::<TombstoneRange>::with_capacity(ordered.len());
    for range in ordered {
        if let Some(last) = normalized.last_mut() {
            if last.end >= range.start {
                last.end = last.end.max(range.end);
                continue;
            }
        }
        normalized.push(range);
    }
    Ok(normalized)
}

fn tombstone_store_dir(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| TOMBSTONES_FILE_NAME.to_string());
    path.with_file_name(format!("{file_name}{TOMBSTONE_STORE_DIR_SUFFIX}"))
}

fn tombstone_shards_dir(path: &Path) -> PathBuf {
    tombstone_store_dir(path).join(TOMBSTONE_SHARDS_DIR_NAME)
}

fn tombstone_shard_index(series_id: SeriesId) -> usize {
    (series_id as usize) % TOMBSTONE_STORE_SHARD_COUNT
}

fn validate_store_manifest(manifest: &TombstoneStoreManifestV2) -> Result<()> {
    if manifest.version != TOMBSTONE_STORE_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone store version {}",
            manifest.version
        )));
    }
    if usize::from(manifest.shard_count) != TOMBSTONE_STORE_SHARD_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone shard count {}",
            manifest.shard_count
        )));
    }
    if manifest.shards.len() != TOMBSTONE_STORE_SHARD_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "invalid tombstone manifest shard count {}",
            manifest.shards.len()
        )));
    }
    for (shard_index, file_name) in manifest.shards.iter().enumerate() {
        if let Some(file_name) = file_name {
            validate_tombstone_shard_file_name(shard_index, file_name)?;
        }
    }
    Ok(())
}

fn validate_tombstone_shard_file_name(shard_index: usize, file_name: &str) -> Result<()> {
    let expected_prefix = format!("shard-{shard_index:03}-");
    let valid = file_name
        .strip_prefix(&expected_prefix)
        .and_then(|value| value.strip_suffix(".bin"))
        .is_some_and(|nonce| {
            nonce.len() == 16
                && nonce
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    if valid {
        Ok(())
    } else {
        Err(TsinkError::DataCorruption(format!(
            "invalid tombstone shard file name for manifest slot {shard_index}: {file_name:?}"
        )))
    }
}

fn encode_store_manifest(manifest: &TombstoneStoreManifestV2) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(TOMBSTONE_STORE_MAGIC.len() + 64);
    payload.extend_from_slice(TOMBSTONE_STORE_MAGIC);
    payload.extend_from_slice(&bincode::serialize(manifest)?);
    Ok(payload)
}

fn take_bincode_bytes<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    len: usize,
    description: &str,
) -> Result<&'a [u8]> {
    let end = position
        .checked_add(len)
        .ok_or_else(|| TsinkError::DataCorruption(format!("{description} length overflow")))?;
    let value = bytes
        .get(*position..end)
        .ok_or_else(|| TsinkError::DataCorruption(format!("truncated {description}")))?;
    *position = end;
    Ok(value)
}

fn read_bincode_u8(bytes: &[u8], position: &mut usize, description: &str) -> Result<u8> {
    Ok(take_bincode_bytes(bytes, position, 1, description)?[0])
}

fn read_bincode_u16(bytes: &[u8], position: &mut usize, description: &str) -> Result<u16> {
    let value: [u8; 2] = take_bincode_bytes(bytes, position, 2, description)?
        .try_into()
        .expect("two-byte slice");
    Ok(u16::from_le_bytes(value))
}

fn read_bincode_u32(bytes: &[u8], position: &mut usize, description: &str) -> Result<u32> {
    let value: [u8; 4] = take_bincode_bytes(bytes, position, 4, description)?
        .try_into()
        .expect("four-byte slice");
    Ok(u32::from_le_bytes(value))
}

fn read_bincode_u64(bytes: &[u8], position: &mut usize, description: &str) -> Result<u64> {
    let value: [u8; 8] = take_bincode_bytes(bytes, position, 8, description)?
        .try_into()
        .expect("eight-byte slice");
    Ok(u64::from_le_bytes(value))
}

fn read_bincode_i64(bytes: &[u8], position: &mut usize, description: &str) -> Result<i64> {
    let value: [u8; 8] = take_bincode_bytes(bytes, position, 8, description)?
        .try_into()
        .expect("eight-byte slice");
    Ok(i64::from_le_bytes(value))
}

fn decode_store_manifest(bytes: &[u8]) -> Result<Option<TombstoneStoreManifestV2>> {
    if !bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
        return Ok(None);
    }
    let encoded = &bytes[TOMBSTONE_STORE_MAGIC.len()..];
    let mut position = 0usize;
    let version = read_bincode_u16(encoded, &mut position, "tombstone manifest version")?;
    let shard_count = read_bincode_u16(encoded, &mut position, "tombstone manifest shard count")?;
    let slot_count = usize::try_from(read_bincode_u64(
        encoded,
        &mut position,
        "tombstone manifest slot count",
    )?)
    .map_err(|_| {
        TsinkError::DataCorruption("unsupported tombstone manifest slot count".to_string())
    })?;
    if slot_count != TOMBSTONE_STORE_SHARD_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "invalid tombstone manifest shard count {slot_count}"
        )));
    }
    let mut shards = Vec::new();
    shards.try_reserve_exact(slot_count).map_err(|_| {
        TsinkError::Other("unable to allocate bounded tombstone manifest slots".to_string())
    })?;
    for shard_index in 0..slot_count {
        match read_bincode_u8(encoded, &mut position, "tombstone manifest option tag")? {
            0 => shards.push(None),
            1 => {
                let len = usize::try_from(read_bincode_u64(
                    encoded,
                    &mut position,
                    "tombstone manifest shard name length",
                )?)
                .map_err(|_| {
                    TsinkError::DataCorruption(
                        "unsupported tombstone manifest shard name length".to_string(),
                    )
                })?;
                // Every accepted name has one exact, bounded shape; reject before allocating.
                if len != "shard-000-0000000000000000.bin".len() {
                    return Err(TsinkError::DataCorruption(format!(
                        "invalid tombstone shard name length {len} in manifest slot {shard_index}"
                    )));
                }
                let name = std::str::from_utf8(take_bincode_bytes(
                    encoded,
                    &mut position,
                    len,
                    "tombstone manifest shard name",
                )?)
                .map_err(|_| {
                    TsinkError::DataCorruption(
                        "tombstone manifest shard name is not valid UTF-8".to_string(),
                    )
                })?
                .to_owned();
                validate_tombstone_shard_file_name(shard_index, &name)?;
                shards.push(Some(name));
            }
            tag => {
                return Err(TsinkError::DataCorruption(format!(
                    "invalid tombstone manifest option tag {tag}"
                )))
            }
        }
    }
    if position != encoded.len() {
        return Err(TsinkError::DataCorruption(
            "trailing bytes in tombstone manifest".to_string(),
        ));
    }
    let manifest = TombstoneStoreManifestV2 {
        version,
        shard_count,
        shards,
    };
    validate_store_manifest(&manifest)?;
    Ok(Some(manifest))
}

fn encode_shard(entries: Vec<TombstoneSeriesEntryV1>) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(TOMBSTONE_SHARD_MAGIC.len() + 128);
    payload.extend_from_slice(TOMBSTONE_SHARD_MAGIC);
    payload.extend_from_slice(&bincode::serialize(&TombstoneShardFileV2 {
        version: TOMBSTONE_STORE_VERSION,
        entries,
    })?);
    Ok(payload)
}

#[derive(Debug, Clone, Copy)]
struct TombstoneShardPreflight {
    entry_count: usize,
    total_range_count: usize,
}

impl TombstoneShardPreflight {
    fn decoded_memory_upper_bound(self, encoded_bytes: usize) -> usize {
        encoded_bytes
            .saturating_add(
                self.entry_count
                    .saturating_mul(TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES),
            )
            .saturating_add(
                self.total_range_count
                    .saturating_mul(std::mem::size_of::<TombstoneRange>())
                    .saturating_mul(2),
            )
            .saturating_add(4096)
    }
}

fn preflight_shard_bytes(
    bytes: &[u8],
    expected_shard_index: Option<usize>,
) -> Result<TombstoneShardPreflight> {
    if !bytes.starts_with(TOMBSTONE_SHARD_MAGIC) {
        return Err(TsinkError::DataCorruption(
            "invalid tombstone shard header".to_string(),
        ));
    }
    let encoded = &bytes[TOMBSTONE_SHARD_MAGIC.len()..];
    let mut position = 0usize;
    let version = read_bincode_u16(encoded, &mut position, "tombstone shard version")?;
    if version != TOMBSTONE_STORE_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone shard version {version}"
        )));
    }
    let entry_count = usize::try_from(read_bincode_u64(
        encoded,
        &mut position,
        "tombstone shard entry count",
    )?)
    .map_err(|_| {
        TsinkError::DataCorruption("unsupported tombstone shard entry count".to_string())
    })?;
    if entry_count > encoded.len().saturating_sub(position) / 16 {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone shard entry count {entry_count} exceeds its encoded bytes"
        )));
    }
    let mut previous_series_id = None;
    let mut total_range_count = 0usize;
    for _ in 0..entry_count {
        let series_id = read_bincode_u64(encoded, &mut position, "tombstone series id")?;
        if previous_series_id.is_some_and(|previous| previous >= series_id) {
            return Err(TsinkError::DataCorruption(
                "tombstone shard series ids must be unique and strictly ordered".to_string(),
            ));
        }
        previous_series_id = Some(series_id);
        if expected_shard_index
            .is_some_and(|shard_index| tombstone_shard_index(series_id) != shard_index)
        {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone series {series_id} is stored in the wrong shard"
            )));
        }
        let range_count = usize::try_from(read_bincode_u64(
            encoded,
            &mut position,
            "tombstone range count",
        )?)
        .map_err(|_| TsinkError::DataCorruption("unsupported tombstone range count".to_string()))?;
        if range_count == 0 {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone series {series_id} has no ranges"
            )));
        }
        if range_count > encoded.len().saturating_sub(position) / 16 {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone range count {range_count} exceeds its encoded bytes"
            )));
        }
        total_range_count = total_range_count.checked_add(range_count).ok_or_else(|| {
            TsinkError::DataCorruption("tombstone shard total range count overflow".to_string())
        })?;
        let mut previous_end = None;
        for _ in 0..range_count {
            let start = read_bincode_i64(encoded, &mut position, "tombstone range start")?;
            let end = read_bincode_i64(encoded, &mut position, "tombstone range end")?;
            if start >= end || previous_end.is_some_and(|previous| previous >= start) {
                return Err(TsinkError::DataCorruption(
                    "tombstone shard ranges must be valid, ordered, and non-overlapping"
                        .to_string(),
                ));
            }
            previous_end = Some(end);
        }
    }
    if position != encoded.len() {
        return Err(TsinkError::DataCorruption(
            "trailing bytes in tombstone shard".to_string(),
        ));
    }
    Ok(TombstoneShardPreflight {
        entry_count,
        total_range_count,
    })
}

fn decode_shard_map(bytes: &[u8], shard_index: usize) -> Result<TombstoneMap> {
    let preflight = preflight_shard_bytes(bytes, Some(shard_index))?;
    let _decoded_memory_upper_bound = preflight.decoded_memory_upper_bound(bytes.len());
    let encoded = &bytes[TOMBSTONE_SHARD_MAGIC.len()..];
    let mut position = 2 + 8;
    let mut tombstones = TombstoneMap::new();
    for _ in 0..preflight.entry_count {
        let series_id = read_bincode_u64(encoded, &mut position, "tombstone series id")?;
        let range_count = usize::try_from(read_bincode_u64(
            encoded,
            &mut position,
            "tombstone range count",
        )?)
        .map_err(|_| TsinkError::DataCorruption("unsupported tombstone range count".to_string()))?;
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(range_count).map_err(|_| {
            TsinkError::Other("unable to allocate bounded tombstone ranges".to_string())
        })?;
        for _ in 0..range_count {
            ranges.push(TombstoneRange {
                start: read_bincode_i64(encoded, &mut position, "tombstone range start")?,
                end: read_bincode_i64(encoded, &mut position, "tombstone range end")?,
            });
        }
        tombstones.insert(series_id, ranges);
    }
    Ok(tombstones)
}

fn empty_store_manifest() -> TombstoneStoreManifestV2 {
    TombstoneStoreManifestV2 {
        version: TOMBSTONE_STORE_VERSION,
        shard_count: TOMBSTONE_STORE_SHARD_COUNT as u16,
        shards: vec![None; TOMBSTONE_STORE_SHARD_COUNT],
    }
}

fn shard_entries_from_map(map: TombstoneMap) -> Vec<TombstoneSeriesEntryV1> {
    let mut entries = map
        .into_iter()
        .map(|(series_id, ranges)| TombstoneSeriesEntryV1 { series_id, ranges })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.series_id);
    entries
}

fn tombstone_shard_index_from_path(path: &Path) -> Result<usize> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "tombstone shard name is not valid UTF-8: {}",
                path.display()
            ))
        })?;
    let shard_index = file_name
        .strip_prefix("shard-")
        .and_then(|name| name.get(..3))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "invalid tombstone shard file name: {}",
                path.display()
            ))
        })?;
    validate_tombstone_shard_file_name(shard_index, file_name)?;
    Ok(shard_index)
}

fn read_shard_map_with_memory_admission(
    path: &Path,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneMap> {
    validate_tombstone_shard_path(path)?;
    let bytes = read_optional_regular_file_bounded_exact_with_admission(
        path,
        MAX_TOMBSTONE_SHARD_BYTES,
        |len| admit_memory(len.saturating_mul(10).saturating_add(4096)),
    )?
    .ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "referenced tombstone shard is missing: {}",
            path.display()
        ))
    })?;
    let shard_index = tombstone_shard_index_from_path(path)?;
    let preflight = preflight_shard_bytes(&bytes, Some(shard_index))?;
    admit_memory(
        preflight
            .decoded_memory_upper_bound(bytes.len())
            .saturating_mul(2),
    )?;
    decode_shard_map(&bytes, shard_index)
}

fn tombstone_map_allocation_bytes(map: &TombstoneMap) -> usize {
    map.len()
        .saturating_mul(TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES)
        .saturating_add(map.values().fold(0usize, |total, ranges| {
            total.saturating_add(
                ranges
                    .capacity()
                    .saturating_mul(std::mem::size_of::<TombstoneRange>()),
            )
        }))
}

fn load_sharded_tombstones_with_memory_admission(
    path: &Path,
    manifest: TombstoneStoreManifestV2,
    manifest_retained_bytes: usize,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneMap> {
    validate_store_manifest(&manifest)?;
    let shards_dir = tombstone_shards_dir(path);
    let mut tombstones = TombstoneMap::new();
    for file_name in manifest.shards.into_iter().flatten() {
        let shard_path = shards_dir.join(file_name);
        let metadata =
            std::fs::symlink_metadata(&shard_path).map_err(|source| TsinkError::IoWithPath {
                path: shard_path.clone(),
                source,
            })?;
        if !metadata.file_type().is_file()
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || metadata.len() > MAX_TOMBSTONE_SHARD_BYTES as u64
        {
            return Err(TsinkError::DataCorruption(format!(
                "referenced tombstone shard has invalid type or length: {}",
                shard_path.display()
            )));
        }
        let expected_len = usize::try_from(metadata.len()).map_err(|_| {
            TsinkError::DataCorruption(format!(
                "referenced tombstone shard length is unsupported: {}",
                shard_path.display()
            ))
        })?;
        admit_memory(
            manifest_retained_bytes
                .saturating_add(tombstone_map_allocation_bytes(&tombstones))
                .saturating_add(expected_len)
                .saturating_add(4096),
        )?;
        validate_tombstone_shard_path(&shard_path)?;
        let bytes = read_required_regular_file_bounded_exact(
            &shard_path,
            MAX_TOMBSTONE_SHARD_BYTES,
            expected_len,
        )?;
        let file_name = shard_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "tombstone shard name is not valid UTF-8: {}",
                    shard_path.display()
                ))
            })?;
        let shard_index = file_name
            .strip_prefix("shard-")
            .and_then(|name| name.get(..3))
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "invalid tombstone shard file name: {}",
                    shard_path.display()
                ))
            })?;
        validate_tombstone_shard_file_name(shard_index, file_name)?;
        let preflight = preflight_shard_bytes(&bytes, Some(shard_index))?;
        let decoded_peak = preflight.decoded_memory_upper_bound(bytes.len());
        admit_memory(
            manifest_retained_bytes
                .saturating_add(tombstone_map_allocation_bytes(&tombstones))
                .saturating_add(decoded_peak),
        )?;
        let loaded = decode_shard_map(&bytes, shard_index)?;
        drop(bytes);
        let loaded_bytes = tombstone_map_allocation_bytes(&loaded);
        admit_memory(
            manifest_retained_bytes
                .saturating_add(tombstone_map_allocation_bytes(&tombstones).saturating_mul(3))
                .saturating_add(loaded_bytes.saturating_mul(2))
                .saturating_add(4096),
        )?;
        for (series_id, ranges) in loaded {
            tombstones.insert(series_id, ranges);
        }
    }
    Ok(tombstones)
}

fn allocate_tombstone_shard_file_name(path: &Path, shard_index: usize) -> Result<String> {
    for _ in 0..256 {
        let file_name = format!(
            "shard-{shard_index:03}-{:016x}.bin",
            TOMBSTONE_SHARD_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let shard_path = tombstone_shards_dir(path).join(&file_name);
        match std::fs::symlink_metadata(&shard_path) {
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(file_name),
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: shard_path,
                    source,
                })
            }
        }
    }

    Err(TsinkError::Other(format!(
        "failed to allocate a unique tombstone shard file for shard {shard_index} beneath {}",
        tombstone_shards_dir(path).display()
    )))
}

fn prepare_shard_file(
    path: &Path,
    shard_index: usize,
    map: TombstoneMap,
) -> Result<Option<PreparedTombstoneShard>> {
    if map.is_empty() {
        return Ok(None);
    }

    let file_name = allocate_tombstone_shard_file_name(path, shard_index)?;
    let shard_path = tombstone_shards_dir(path).join(&file_name);
    let payload = encode_shard(shard_entries_from_map(map))?;
    Ok(Some(PreparedTombstoneShard {
        file_name,
        path: shard_path,
        payload,
    }))
}

fn tombstone_shard_record(
    shard_index: usize,
    file_name: &str,
    payload: &[u8],
) -> Result<TombstoneTransactionShardRecord> {
    validate_tombstone_shard_file_name(shard_index, file_name)?;
    preflight_shard_bytes(payload, Some(shard_index))?;
    Ok(TombstoneTransactionShardRecord {
        shard_index: u16::try_from(shard_index)
            .map_err(|_| TsinkError::Other("tombstone shard index exceeds u16".to_string()))?,
        file_name: file_name.to_string(),
        logical_bytes: u64::try_from(payload.len())
            .map_err(|_| TsinkError::Other("tombstone shard payload exceeds u64".to_string()))?,
        xxh64: xxhash_rust::xxh64::xxh64(payload, 0),
    })
}

fn fingerprint_candidate_manifest_shards(
    path: &Path,
    manifest: &TombstoneStoreManifestV2,
    new_shards: &[PreparedTombstoneShard],
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<Vec<TombstoneTransactionShardRecord>> {
    let new_payloads = new_shards
        .iter()
        .map(|shard| (shard.file_name.as_str(), shard.payload.as_slice()))
        .collect::<HashMap<_, _>>();
    let mut records = Vec::new();
    for (shard_index, file_name) in manifest.shards.iter().enumerate() {
        let Some(file_name) = file_name else {
            continue;
        };
        let record = match new_payloads.get(file_name.as_str()) {
            Some(payload) => tombstone_shard_record(shard_index, file_name, payload)?,
            None => {
                let shard_path = tombstone_shards_dir(path).join(file_name);
                let payload = read_optional_regular_file_bounded_exact_with_admission(
                    &shard_path,
                    MAX_TOMBSTONE_SHARD_BYTES,
                    |len| admit_memory(len.saturating_mul(2).saturating_add(4096)),
                )?
                .ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "candidate tombstone shard disappeared while fingerprinting: {}",
                        shard_path.display()
                    ))
                })?;
                preflight_shard_bytes(&payload, Some(shard_index))?;
                tombstone_shard_record(shard_index, file_name, &payload)?
            }
        };
        records.push(record);
    }
    Ok(records)
}

/// Removes only an exact, no-follow regular-file target and reports whether the directory entry
/// actually disappeared. In particular this never falls back to recursive directory removal if
/// an owned filename is replaced between discovery and cleanup.
fn remove_owned_regular_file_and_sync_parent_observed(path: &Path) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
    {
        return Err(TsinkError::DataCorruption(format!(
            "owned tombstone cleanup target must be a regular file and may not be link-like: {}",
            path.display()
        )));
    }
    let removed = remove_file_if_exists(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if removed {
        crate::engine::fs_utils::sync_parent_dir(path)?;
    }
    Ok(removed)
}

/// Removes a preflighted sequence of exact tombstone files under one lazy governed reservation.
///
/// Cleanup stops at the first per-entry error, preserving caller ordering. A successful no-op
/// batch avoids a root scan; any governed unlink, ambiguous removal/sync failure, or settlement
/// failure receives exactly one terminal reconciliation before the result is returned.
fn remove_owned_regular_files_and_sync_parents_budgeted<'a>(
    paths: impl IntoIterator<Item = &'a Path>,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
    mut validate_before_remove: impl FnMut(&Path) -> Result<()>,
) -> Result<u64> {
    let mut reservation = None;
    let mut governed_mutation_or_ambiguity = false;
    let mut removed = 0u64;
    let removal_result = (|| -> Result<()> {
        for path in paths {
            validate_before_remove(path)?;
            let governed = match budget {
                Some(budget) => budget.governs_entry(path)?,
                None => false,
            };
            if governed && reservation.is_none() {
                reservation = Some(
                    budget
                        .expect("governed tombstone cleanup requires a disk budget")
                        .reserve(category, 0, crate::DiskReservationKind::Recovery)?,
                );
            }
            match remove_owned_regular_file_and_sync_parent_observed(path) {
                Ok(was_removed) => {
                    governed_mutation_or_ambiguity |= governed && was_removed;
                    if was_removed {
                        removed = removed.saturating_add(1);
                    }
                }
                Err(err) => {
                    governed_mutation_or_ambiguity |= governed;
                    return Err(err);
                }
            }
        }
        Ok(())
    })();

    let settlement_result = reservation
        .map(|reservation| reservation.commit(0, 0))
        .transpose()
        .map(|_| ());
    if settlement_result.is_err() {
        governed_mutation_or_ambiguity = true;
    }
    let reconciliation_result = if governed_mutation_or_ambiguity {
        budget
            .expect("governed tombstone cleanup requires a disk budget")
            .reconcile_when_idle()
            .map(|_| ())
    } else {
        Ok(())
    };

    let mut errors = Vec::new();
    if let Err(err) = &removal_result {
        errors.push(format!("cleanup failed: {err}"));
    }
    if let Err(err) = &settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = &reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    match errors.len() {
        0 => Ok(removed),
        1 => match (removal_result, settlement_result, reconciliation_result) {
            (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
            _ => unreachable!("one recorded tombstone cleanup error must have a failed result"),
        },
        _ => Err(TsinkError::Other(format!(
            "batched budgeted exact-file tombstone cleanup failed: {}",
            errors.join("; ")
        ))),
    }
}

fn remove_owned_regular_file_and_sync_parent_budgeted(
    path: &Path,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
) -> Result<bool> {
    remove_owned_regular_files_and_sync_parents_budgeted(
        std::iter::once(path),
        budget,
        category,
        |_| Ok(()),
    )
    .map(|removed| removed != 0)
}

fn cleanup_replaced_shards(
    path: &Path,
    previous: &[Option<String>],
    next: &[Option<String>],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let shards_dir = tombstone_shards_dir(path);
    for (shard_index, file_name) in previous.iter().enumerate() {
        if let Some(file_name) = file_name {
            validate_tombstone_shard_file_name(shard_index, file_name)?;
        }
    }
    for (shard_index, file_name) in next.iter().enumerate() {
        if let Some(file_name) = file_name {
            validate_tombstone_shard_file_name(shard_index, file_name)?;
        }
    }
    let keep = next.iter().flatten().cloned().collect::<HashSet<_>>();
    for file_name in previous.iter().flatten() {
        if keep.contains(file_name) {
            continue;
        }
        let shard_path = shards_dir.join(file_name);
        match std::fs::symlink_metadata(&shard_path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(TsinkError::DataCorruption(format!(
                    "obsolete tombstone shard must be a regular file and may not be a symlink: {}",
                    shard_path.display()
                )))
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: shard_path,
                    source,
                })
            }
        }
        remove_owned_regular_file_and_sync_parent_budgeted(
            &shard_path,
            local_disk_budget,
            crate::DiskCategory::Tombstones,
        )?;
    }
    Ok(())
}

fn load_store_manifest_from_bytes(bytes: &[u8]) -> Result<Option<TombstoneStoreManifestV2>> {
    match decode_store_manifest(bytes)? {
        Some(manifest) => Ok(Some(manifest)),
        None => Ok(None),
    }
}

fn load_legacy_tombstones_from_bytes(bytes: &[u8]) -> Result<TombstoneMap> {
    let snapshot: TombstoneFileV1 = serde_json::from_slice(bytes)?;
    if snapshot.version != TOMBSTONES_FILE_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone index version {}",
            snapshot.version
        )));
    }

    let mut tombstones = TombstoneMap::new();
    for entry in snapshot.entries {
        for range in entry.ranges {
            merge_tombstone_range(tombstones.entry(entry.series_id).or_default(), range);
        }
    }
    normalize_tombstone_map(&mut tombstones)?;
    Ok(tombstones)
}

pub(crate) fn remote_tombstone_manifest_file_len(lane: &TombstoneLane) -> Result<Option<usize>> {
    validate_tombstone_lane_namespace(lane)?;
    let metadata = match std::fs::symlink_metadata(&lane.manifest_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: lane.manifest_path.clone(),
                source,
            })
        }
    };
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || metadata.len() > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "remote tombstone manifest has invalid type or length: {}",
            lane.manifest_path.display()
        )));
    }
    usize::try_from(metadata.len()).map(Some).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "remote tombstone manifest size is unsupported: {}",
            lane.manifest_path.display()
        ))
    })
}

fn remote_tombstone_manifest_fingerprint(
    bytes: Option<&[u8]>,
) -> RemoteTombstoneManifestFingerprint {
    match bytes {
        Some(bytes) => RemoteTombstoneManifestFingerprint {
            exists: true,
            logical_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            xxh64: xxhash_rust::xxh64::xxh64(bytes, 0),
        },
        None => RemoteTombstoneManifestFingerprint {
            exists: false,
            logical_bytes: 0,
            xxh64: 0,
        },
    }
}

fn read_remote_tombstone_manifest_bytes(
    lane: &TombstoneLane,
    expected_len: Option<usize>,
) -> Result<Option<Vec<u8>>> {
    validate_tombstone_lane_namespace(lane)?;
    match expected_len {
        Some(expected_len) => read_required_regular_file_bounded_exact(
            &lane.manifest_path,
            MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
            expected_len,
        )
        .map(Some),
        None => match std::fs::symlink_metadata(&lane.manifest_path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Ok(_) => Err(TsinkError::DataCorruption(format!(
                "remote tombstone manifest appeared after its missing-file probe: {}",
                lane.manifest_path.display()
            ))),
            Err(source) => Err(TsinkError::IoWithPath {
                path: lane.manifest_path.clone(),
                source,
            }),
        },
    }
}

pub(crate) fn load_remote_tombstone_manifest_snapshot(
    lane: &TombstoneLane,
    expected_len: Option<usize>,
) -> Result<RemoteTombstoneManifestSnapshot> {
    let bytes = read_remote_tombstone_manifest_bytes(lane, expected_len)?;
    let fingerprint = remote_tombstone_manifest_fingerprint(bytes.as_deref());
    let kind = match bytes {
        None => RemoteTombstoneManifestKind::Missing,
        Some(bytes) => match load_store_manifest_from_bytes(&bytes)? {
            Some(manifest) => RemoteTombstoneManifestKind::Sharded(manifest.shards),
            // Finite remote refresh must not decode or repartition a complete legacy map in one
            // maintenance item. The caller rejects this compatibility format and asks operators
            // to migrate it through the explicit unlimited/startup path.
            None => RemoteTombstoneManifestKind::Legacy,
        },
    };
    Ok(RemoteTombstoneManifestSnapshot { fingerprint, kind })
}

pub(crate) fn revalidate_remote_tombstone_manifest(
    lane: &TombstoneLane,
    expected_len: Option<usize>,
) -> Result<RemoteTombstoneManifestFingerprint> {
    let bytes = read_remote_tombstone_manifest_bytes(lane, expected_len)?;
    Ok(remote_tombstone_manifest_fingerprint(bytes.as_deref()))
}

pub(crate) fn remote_tombstone_shard_file_len(
    lane: &TombstoneLane,
    shard_index: usize,
    file_name: &str,
) -> Result<usize> {
    validate_tombstone_lane_namespace(lane)?;
    validate_tombstone_shard_file_name(shard_index, file_name)?;
    let shard_path = tombstone_shards_dir(&lane.manifest_path).join(file_name);
    validate_tombstone_shard_path(&shard_path)?;
    let metadata =
        std::fs::symlink_metadata(&shard_path).map_err(|source| TsinkError::IoWithPath {
            path: shard_path.clone(),
            source,
        })?;
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || metadata.len() > MAX_TOMBSTONE_SHARD_BYTES as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "referenced remote tombstone shard has invalid type or length: {}",
            shard_path.display()
        )));
    }
    usize::try_from(metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "referenced remote tombstone shard length is unsupported: {}",
            shard_path.display()
        ))
    })
}

pub(crate) fn load_remote_tombstone_shard_with_memory_admission(
    lane: &TombstoneLane,
    shard_index: usize,
    file_name: &str,
    expected_len: usize,
    admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneMap> {
    validate_tombstone_shard_file_name(shard_index, file_name)?;
    let shard_path = tombstone_shards_dir(&lane.manifest_path).join(file_name);
    validate_tombstone_shard_path(&shard_path)?;
    let mut admit_memory = admit_memory;
    admit_memory(expected_len.saturating_mul(10).saturating_add(4096))?;
    let bytes = read_required_regular_file_bounded_exact(
        &shard_path,
        MAX_TOMBSTONE_SHARD_BYTES,
        expected_len,
    )?;
    let preflight = preflight_shard_bytes(&bytes, Some(shard_index))?;
    admit_memory(
        preflight
            .decoded_memory_upper_bound(bytes.len())
            .saturating_mul(2),
    )?;
    decode_shard_map(&bytes, shard_index)
}

pub(crate) fn load_tombstones_with_memory_admission(
    path: &Path,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneMap> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(TombstoneMap::new()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || metadata.len() > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone manifest has invalid type or length: {}",
            path.display()
        )));
    }
    let manifest_len = usize::try_from(metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "tombstone manifest size is unsupported: {}",
            path.display()
        ))
    })?;
    admit_memory(manifest_len.saturating_add(4096))?;
    let bytes = read_required_regular_file_bounded_exact(
        path,
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
        manifest_len,
    )?;

    admit_memory(
        manifest_len
            .saturating_mul(if bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
                8
            } else {
                64
            })
            .saturating_add(4096),
    )?;
    if let Some(manifest) = load_store_manifest_from_bytes(&bytes)? {
        let manifest_retained_bytes = manifest_len.saturating_mul(8).saturating_add(4096);
        return load_sharded_tombstones_with_memory_admission(
            path,
            manifest,
            manifest_retained_bytes,
            admit_memory,
        );
    }
    load_legacy_tombstones_from_bytes(&bytes)
}

pub(crate) fn load_tombstones(path: &Path) -> Result<TombstoneMap> {
    load_tombstones_with_memory_admission(path, |_| Ok(()))
}

/// Conservative peak for one lane while its encoded files, decoded map, and normalization
/// scratch coexist. V2 files have fixed-width framing, so eight encoded bytes per retained byte
/// covers the ordered-map/vector allocation model; legacy JSON uses a wider factor for punctuation-
/// dense arrays. Callers combine this with the already-accounted live tombstone map.
pub(crate) fn tombstone_manifest_probe_memory_upper_bound(path: &Path) -> Result<usize> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    if !metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
    {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone manifest must be a regular file and may not be link-like: {}",
            path.display()
        )));
    }
    let len = usize::try_from(metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "tombstone manifest size is unsupported: {}",
            path.display()
        ))
    })?;
    if len > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone manifest exceeds its {}-byte bound: {}",
            MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
            path.display()
        )));
    }
    Ok(len.saturating_mul(64).saturating_add(4096))
}

pub(crate) fn tombstone_transaction_probe_memory_upper_bound(
    lanes: &[TombstoneLane],
) -> Result<usize> {
    if lanes.is_empty() {
        return Ok(0);
    }
    let lanes = normalize_tombstone_lanes(lanes)?;
    validate_tombstone_lanes(&lanes)?;
    lanes.iter().try_fold(0usize, |total, lane| {
        tombstone_manifest_probe_memory_upper_bound(&lane.manifest_path)
            .map(|bytes| total.saturating_add(bytes))
    })
}

pub(crate) fn tombstone_transaction_staging_memory_upper_bound_with_admission(
    lanes: &[TombstoneLane],
    updates: &TombstoneMap,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<usize> {
    let update_map_bytes = tombstone_map_allocation_bytes(updates);
    let mut total = update_map_bytes.saturating_mul(4).saturating_add(16 * 1024);
    admit_memory(total)?;
    if lanes.is_empty() {
        return Ok(total);
    }
    let lanes = normalize_tombstone_lanes(lanes)?;
    validate_tombstone_lanes(&lanes)?;
    let touched_shards = updates
        .keys()
        .map(|series_id| tombstone_shard_index(*series_id))
        .collect::<HashSet<_>>();
    for lane in lanes {
        let retained_before_manifest = total;
        let manifest_bytes = read_optional_regular_file_bounded_exact_with_admission(
            &lane.manifest_path,
            MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
            |len| {
                admit_memory(
                    retained_before_manifest
                        .saturating_add(len)
                        .saturating_add(4096),
                )
            },
        )?;
        if let Some(bytes) = manifest_bytes.as_ref() {
            admit_memory(
                retained_before_manifest
                    .saturating_add(bytes.len().saturating_mul(
                        if bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
                            8
                        } else {
                            64
                        },
                    ))
                    .saturating_add(4096),
            )?;
        }
        total = total.saturating_add(manifest_bytes.as_ref().map_or(0, |bytes| {
            bytes
                .len()
                .saturating_mul(if bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
                    8
                } else {
                    64
                })
        }));
        let manifest = manifest_bytes
            .as_deref()
            .map(load_store_manifest_from_bytes)
            .transpose()?
            .flatten();
        if let Some(manifest) = manifest {
            let mut untouched_fingerprint_peak = 0usize;
            for (shard_index, file_name) in manifest.shards.iter().enumerate() {
                let Some(file_name) = file_name.as_deref() else {
                    continue;
                };
                validate_tombstone_shard_file_name(shard_index, file_name)?;
                let shard_path = tombstone_shards_dir(&lane.manifest_path).join(file_name);
                let metadata = std::fs::symlink_metadata(&shard_path).map_err(|source| {
                    TsinkError::IoWithPath {
                        path: shard_path.clone(),
                        source,
                    }
                })?;
                if !metadata.file_type().is_file()
                    || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || metadata.len() > MAX_TOMBSTONE_SHARD_BYTES as u64
                {
                    return Err(TsinkError::DataCorruption(format!(
                        "tombstone update source shard has invalid type or length: {}",
                        shard_path.display()
                    )));
                }
                let len = usize::try_from(metadata.len()).map_err(|_| {
                    TsinkError::DataCorruption(format!(
                        "tombstone update source shard length is unsupported: {}",
                        shard_path.display()
                    ))
                })?;
                if touched_shards.contains(&shard_index) {
                    // Decoded source map, replacement payload, fingerprint bytes, and allocator
                    // growth can coexist until the multi-lane plan is durably decided.
                    total = total.saturating_add(len.saturating_mul(10).saturating_add(4096));
                } else {
                    // Fingerprinting reuses one bounded payload at a time for unchanged shards.
                    untouched_fingerprint_peak =
                        untouched_fingerprint_peak.max(len.saturating_mul(2).saturating_add(4096));
                }
            }
            total = total.saturating_add(untouched_fingerprint_peak);
        }
        total = total.saturating_add(update_map_bytes.saturating_mul(4));
        admit_memory(total)?;
    }
    Ok(total)
}

#[cfg(test)]
fn ordered_tombstone_map<'a, M>(map: &'a M) -> TombstoneMap
where
    &'a M: IntoIterator<Item = (&'a SeriesId, &'a Vec<TombstoneRange>)>,
{
    map.into_iter()
        .map(|(&series_id, ranges)| (series_id, ranges.clone()))
        .collect()
}

#[cfg(test)]
pub(crate) fn persist_tombstones<'a, M>(path: &Path, tombstones: &'a M) -> Result<()>
where
    &'a M: IntoIterator<Item = (&'a SeriesId, &'a Vec<TombstoneRange>)>,
{
    let tombstones = ordered_tombstone_map(tombstones);
    let data_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let lanes = [TombstoneLane {
        role: TombstoneLaneRole::LocalNumeric,
        namespace_root: data_path.clone(),
        manifest_path: path.to_path_buf(),
    }];
    persist_tombstone_snapshot_transactionally(&data_path, &lanes, &tombstones, None)
        .map_err(TombstonePersistenceError::into_tsink_error)
}

fn prepare_tombstone_store_update(
    lane: &TombstoneLane,
    normalized_updates: &TombstoneMap,
    admit_memory: &mut impl FnMut(usize) -> Result<()>,
) -> Result<PreparedTombstoneStoreUpdate> {
    let path = &lane.manifest_path;
    let previous_bytes = read_optional_regular_file_bounded_exact_with_admission(
        path,
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
        |len| admit_memory(len.saturating_add(4096)),
    )?;
    if let Some(bytes) = previous_bytes.as_ref() {
        admit_memory(
            bytes
                .len()
                .saturating_mul(if bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
                    8
                } else {
                    64
                })
                .saturating_add(4096),
        )?;
    }
    let previous_manifest = previous_bytes
        .as_deref()
        .map(load_store_manifest_from_bytes)
        .transpose()?
        .flatten();
    let previous_retained = previous_bytes.as_ref().map_or(0, Vec::capacity);

    let (next_manifest, new_shards) = if let Some(previous_manifest) = &previous_manifest {
        let mut next_manifest = previous_manifest.clone();
        let mut updates_by_shard = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
        for (&series_id, ranges) in normalized_updates {
            updates_by_shard[tombstone_shard_index(series_id)].insert(series_id, ranges.clone());
        }

        let mut new_shards = Vec::new();
        for (shard_index, shard_updates) in updates_by_shard.into_iter().enumerate() {
            if shard_updates.is_empty() {
                continue;
            }

            let mut shard_map = match &previous_manifest.shards[shard_index] {
                Some(file_name) => {
                    let retained = previous_retained.saturating_add(
                        new_shards.iter().fold(0usize, |total, shard| {
                            total.saturating_add(prepared_shard_retained_bytes(shard))
                        }),
                    );
                    read_shard_map_with_memory_admission(
                        &tombstone_shards_dir(path).join(file_name),
                        |scratch| admit_memory(retained.saturating_add(scratch)),
                    )?
                }
                None => TombstoneMap::new(),
            };
            for (series_id, ranges) in shard_updates {
                if ranges.is_empty() {
                    shard_map.remove(&series_id);
                } else {
                    shard_map.insert(series_id, ranges);
                }
            }
            normalize_tombstone_map(&mut shard_map)?;
            let prepared = prepare_shard_file(path, shard_index, shard_map)?;
            next_manifest.shards[shard_index] =
                prepared.as_ref().map(|shard| shard.file_name.clone());
            if let Some(prepared) = prepared {
                new_shards.push(prepared);
            }
        }
        (next_manifest, new_shards)
    } else {
        let update_bytes = tombstone_map_allocation_bytes(normalized_updates);
        let mut merged = load_tombstones_with_memory_admission(path, |bytes| {
            admit_memory(
                previous_retained
                    .saturating_add(update_bytes.saturating_mul(3))
                    .saturating_add(bytes),
            )
        })?;
        for (&series_id, ranges) in normalized_updates {
            merged.insert(series_id, ranges.clone());
        }
        normalize_tombstone_map(&mut merged)?;

        let mut shards = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
        for (series_id, ranges) in merged {
            shards[tombstone_shard_index(series_id)].insert(series_id, ranges);
        }

        let mut next_manifest = empty_store_manifest();
        let mut new_shards = Vec::new();
        for (shard_index, shard_map) in shards.into_iter().enumerate() {
            let prepared = prepare_shard_file(path, shard_index, shard_map)?;
            next_manifest.shards[shard_index] =
                prepared.as_ref().map(|shard| shard.file_name.clone());
            if let Some(prepared) = prepared {
                new_shards.push(prepared);
            }
        }
        (next_manifest, new_shards)
    };

    let next_manifest_payload = encode_store_manifest(&next_manifest)?;
    let candidate_retained = previous_retained
        .saturating_add(next_manifest_payload.capacity())
        .saturating_add(new_shards.iter().fold(0usize, |total, shard| {
            total.saturating_add(prepared_shard_retained_bytes(shard))
        }));
    let candidate_shards =
        fingerprint_candidate_manifest_shards(path, &next_manifest, &new_shards, |scratch| {
            admit_memory(candidate_retained.saturating_add(scratch))
        })?;
    Ok(PreparedTombstoneStoreUpdate {
        lane: lane.clone(),
        path: path.to_path_buf(),
        previous_bytes,
        next_manifest_payload: Some(next_manifest_payload),
        new_shards,
        candidate_shards,
    })
}

fn prepare_tombstone_store_snapshot(
    lane: &TombstoneLane,
    normalized_snapshot: &TombstoneMap,
    admit_memory: &mut impl FnMut(usize) -> Result<()>,
) -> Result<PreparedTombstoneStoreUpdate> {
    let path = &lane.manifest_path;
    let previous_bytes = read_optional_regular_file_bounded_exact_with_admission(
        path,
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
        |len| admit_memory(len.saturating_add(4096)),
    )?;
    if let Some(bytes) = previous_bytes.as_ref() {
        admit_memory(
            bytes
                .len()
                .saturating_mul(if bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
                    8
                } else {
                    64
                })
                .saturating_add(4096),
        )?;
    }
    validated_manifest_image(previous_bytes.as_deref(), false)?;
    let previous_retained = previous_bytes.as_ref().map_or(0, Vec::capacity);

    let mut next_manifest = empty_store_manifest();
    let mut new_shards = Vec::new();
    if !normalized_snapshot.is_empty() {
        let mut shards = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
        for (&series_id, ranges) in normalized_snapshot {
            shards[tombstone_shard_index(series_id)].insert(series_id, ranges.clone());
        }
        for (shard_index, shard_map) in shards.into_iter().enumerate() {
            let prepared = prepare_shard_file(path, shard_index, shard_map)?;
            next_manifest.shards[shard_index] =
                prepared.as_ref().map(|shard| shard.file_name.clone());
            if let Some(prepared) = prepared {
                new_shards.push(prepared);
            }
        }
    }

    let next_manifest_payload = if normalized_snapshot.is_empty() {
        None
    } else {
        let payload = encode_store_manifest(&next_manifest)?;
        Some(payload)
    };
    let candidate_shards = if normalized_snapshot.is_empty() {
        Vec::new()
    } else {
        let candidate_retained = previous_retained
            .saturating_add(next_manifest_payload.as_ref().map_or(0, Vec::capacity))
            .saturating_add(new_shards.iter().fold(0usize, |total, shard| {
                total.saturating_add(prepared_shard_retained_bytes(shard))
            }));
        fingerprint_candidate_manifest_shards(path, &next_manifest, &new_shards, |scratch| {
            admit_memory(candidate_retained.saturating_add(scratch))
        })?
    };
    Ok(PreparedTombstoneStoreUpdate {
        lane: lane.clone(),
        path: path.to_path_buf(),
        previous_bytes,
        next_manifest_payload,
        new_shards,
        candidate_shards,
    })
}

fn checked_add_payload_bytes(total: &mut u64, bytes: usize, description: &str) -> Result<()> {
    let bytes = u64::try_from(bytes).map_err(|_| {
        TsinkError::Other(format!(
            "{description} exceeds the supported tombstone transaction byte range"
        ))
    })?;
    *total = total.checked_add(bytes).ok_or_else(|| {
        TsinkError::Other(format!(
            "tombstone transaction byte total overflowed while adding {description}"
        ))
    })?;
    Ok(())
}

fn validated_manifest_image(
    bytes: Option<&[u8]>,
    require_sharded: bool,
) -> Result<Option<TombstoneStoreManifestV2>> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    match load_store_manifest_from_bytes(bytes)? {
        Some(manifest) => Ok(Some(manifest)),
        None if require_sharded => Err(TsinkError::DataCorruption(
            "tombstone transaction candidate is not a sharded manifest".to_string(),
        )),
        None => {
            load_legacy_tombstones_from_bytes(bytes)?;
            Ok(None)
        }
    }
}

fn validate_transaction_record_against_lanes(
    record: &TombstoneTransactionRecord,
    lanes: &[TombstoneLane],
) -> Result<()> {
    validate_tombstone_lanes(lanes)?;
    if record.transaction_id == 0 {
        return Err(TsinkError::DataCorruption(
            "tombstone transaction coordinator has a zero transaction id".to_string(),
        ));
    }
    if record.lanes.len() != lanes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone transaction lane-set mismatch: coordinator has {}, configuration has {}",
            record.lanes.len(),
            lanes.len()
        )));
    }
    let mut record_roles = HashSet::new();
    let mut record_paths = HashSet::new();
    for (record_lane, configured_lane) in record.lanes.iter().zip(lanes) {
        if !record_roles.insert(record_lane.role)
            || !record_paths.insert(record_lane.manifest_path_identity.clone())
        {
            return Err(TsinkError::DataCorruption(
                "tombstone transaction coordinator contains duplicate lane identities".to_string(),
            ));
        }
        if record_lane.role != configured_lane.role
            || record_lane.namespace_root_identity != configured_lane.namespace_root
            || record_lane.manifest_path_identity != configured_lane.manifest_path
        {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone transaction lane-set mismatch at {:?}",
                configured_lane.role
            )));
        }
        validated_manifest_image(record_lane.previous_manifest.as_deref(), false)?;
        let candidate = validated_manifest_image(record_lane.candidate_manifest.as_deref(), true)?;
        let expected_count = candidate
            .as_ref()
            .map(|manifest| manifest.shards.iter().flatten().count())
            .unwrap_or(0);
        if record_lane.candidate_shards.len() != expected_count {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone transaction shard fingerprint count mismatch for {:?}",
                record_lane.role
            )));
        }
        let mut fingerprint_names = HashSet::new();
        for shard in &record_lane.candidate_shards {
            let shard_index = usize::from(shard.shard_index);
            validate_tombstone_shard_file_name(shard_index, &shard.file_name)?;
            if shard.logical_bytes > MAX_TOMBSTONE_SHARD_BYTES as u64
                || !fingerprint_names.insert(shard.file_name.clone())
                || candidate
                    .as_ref()
                    .and_then(|manifest| manifest.shards.get(shard_index))
                    .and_then(Option::as_deref)
                    != Some(shard.file_name.as_str())
            {
                return Err(TsinkError::DataCorruption(format!(
                    "invalid tombstone transaction shard fingerprint for {:?}: {}",
                    record_lane.role, shard.file_name
                )));
            }
        }
    }
    Ok(())
}

fn transaction_record_from_plans(
    plans: &[PreparedTombstoneStoreUpdate],
    phase: TombstoneTransactionPhase,
) -> TombstoneTransactionRecord {
    TombstoneTransactionRecord {
        version: TOMBSTONE_TRANSACTION_VERSION,
        transaction_id: TOMBSTONE_TRANSACTION_COUNTER.fetch_add(1, Ordering::Relaxed),
        phase,
        lanes: plans
            .iter()
            .map(|plan| TombstoneTransactionLaneRecord {
                role: plan.lane.role,
                namespace_root_identity: plan.lane.namespace_root.clone(),
                manifest_path_identity: plan.lane.manifest_path.clone(),
                previous_manifest: plan.previous_bytes.clone(),
                candidate_manifest: plan.next_manifest_payload.clone(),
                candidate_shards: plan.candidate_shards.clone(),
            })
            .collect(),
    }
}

fn read_tombstone_transaction_record(
    data_path: &Path,
) -> Result<Option<TombstoneTransactionRecord>> {
    read_optional_regular_file_bounded(
        &tombstone_transaction_path(data_path),
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
    )?
    .as_deref()
    .map(decode_tombstone_transaction_record)
    .transpose()
}

fn tombstone_transaction_budget_bytes(
    data_path: &Path,
    plans: &[PreparedTombstoneStoreUpdate],
    coordinator_record_bytes: usize,
    budget: &Arc<crate::LocalDiskBudget>,
) -> Result<u64> {
    let mut bytes = 0u64;
    let mut governed_targets = Vec::new();
    let coordinator_path = tombstone_transaction_path(data_path);
    let mut governed_coordinator = false;
    let mut account_target = |target: &Path, payload_len: usize, is_coordinator: bool| {
        if !budget.governs_entry(target)? {
            return Ok(());
        }
        budget.validate_managed_file_path(target)?;
        governed_targets.push(target.to_path_buf());
        if is_coordinator {
            governed_coordinator = true;
            checked_add_payload_bytes(
                &mut bytes,
                coordinator_record_bytes,
                "tombstone coordinator record",
            )?;
        } else {
            checked_add_payload_bytes(&mut bytes, payload_len, "tombstone transaction payload")?;
        }
        Ok::<(), TsinkError>(())
    };
    for plan in plans {
        for shard in &plan.new_shards {
            account_target(&shard.path, shard.payload.len(), false)?;
        }
        if let Some(payload) = &plan.next_manifest_payload {
            account_target(&plan.path, payload.len(), false)?;
        }
    }
    account_target(&coordinator_path, 0, true)?;

    let entry_allowance = budget.snapshot_restore_entry_staging_allowance_bytes()?;
    let mut atomic_target_count = u64::try_from(governed_targets.len()).map_err(|_| {
        TsinkError::Other("tombstone transaction target count exceeds u64".to_string())
    })?;
    if governed_coordinator {
        // The Committing temporary coexists with the durable Prepared image until rename.
        atomic_target_count = atomic_target_count.checked_add(1).ok_or_else(|| {
            TsinkError::Other("tombstone coordinator entry allowance overflow".to_string())
        })?;
    }
    let missing_parent_count = budget.missing_managed_parent_directory_count(&governed_targets)?;
    let allowance_count = atomic_target_count
        .checked_add(missing_parent_count)
        .ok_or_else(|| {
            TsinkError::Other("tombstone transaction entry allowance overflow".to_string())
        })?;
    bytes = bytes
        .checked_add(
            allowance_count
                .checked_mul(entry_allowance)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "tombstone transaction entry byte allowance overflow".to_string(),
                    )
                })?,
        )
        .ok_or_else(|| {
            TsinkError::Other("tombstone transaction admission byte overflow".to_string())
        })?;
    Ok(bytes)
}

fn prepare_tombstone_transaction_directories(
    data_path: &Path,
    plans: &[PreparedTombstoneStoreUpdate],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let mut prepared = HashSet::new();
    let mut prepare_target = |target: &Path| -> Result<()> {
        let Some(parent) = target.parent() else {
            return Err(TsinkError::InvalidConfiguration(format!(
                "tombstone transaction target has no parent: {}",
                target.display()
            )));
        };
        if !prepared.insert(parent.to_path_buf()) {
            return Ok(());
        }
        match budget {
            Some(budget) if budget.governs_entry(target)? => {
                budget.create_dir_all_and_sync_parents(parent)?;
            }
            _ => {
                crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;
            }
        }
        Ok(())
    };
    for plan in plans {
        for shard in &plan.new_shards {
            prepare_target(&shard.path)?;
        }
        if plan.next_manifest_payload.is_some() {
            prepare_target(&plan.path)?;
        }
    }
    prepare_target(&tombstone_transaction_path(data_path))?;
    Ok(())
}

fn settle_tombstone_transaction_reservation(
    reservation: Option<crate::disk_budget::DiskReservation>,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let settlement = match reservation {
        Some(reservation) => {
            let admitted = reservation.reserved_bytes();
            reservation.commit(admitted, 0)
        }
        None => Ok(()),
    };
    let reconciliation = budget
        .map(|budget| budget.reconcile_when_idle().map(|_| ()))
        .unwrap_or(Ok(()));
    match (settlement, reconciliation) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
        (Err(settlement_err), Err(reconciliation_err)) => Err(TsinkError::Other(format!(
            "tombstone transaction accounting settlement failed: {settlement_err}; reconciliation failed: {reconciliation_err}"
        ))),
    }
}

fn remove_owned_prepared_tombstone_shards(
    plans: &[PreparedTombstoneStoreUpdate],
    owned_paths: &HashSet<PathBuf>,
) -> Result<()> {
    let mut errors = Vec::new();
    for plan in plans.iter().rev() {
        for shard in plan.new_shards.iter().rev() {
            if !owned_paths.contains(&shard.path) {
                continue;
            }
            let metadata = match std::fs::symlink_metadata(&shard.path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    errors.push(format!(
                        "inspect {} failed: {}",
                        shard.path.display(),
                        TsinkError::IoWithPath {
                            path: shard.path.clone(),
                            source,
                        }
                    ));
                    continue;
                }
            };
            if !metadata.file_type().is_file() {
                errors.push(format!(
                    "owned candidate shard changed into a link-like or wrong-type entry: {}",
                    shard.path.display()
                ));
                continue;
            }
            if metadata.len() != shard.payload.len() as u64 {
                errors.push(format!(
                    "owned candidate shard length changed before rollback: {}",
                    shard.path.display()
                ));
                continue;
            }
            let bytes = match read_optional_regular_file_bounded(&shard.path, shard.payload.len()) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(err) => {
                    errors.push(format!("read {} failed: {err}", shard.path.display()));
                    continue;
                }
            };
            if bytes != shard.payload {
                errors.push(format!(
                    "owned candidate shard payload changed before rollback: {}",
                    shard.path.display()
                ));
                continue;
            }
            if let Err(source) = std::fs::remove_file(&shard.path) {
                errors.push(format!(
                    "remove {} failed: {}",
                    shard.path.display(),
                    TsinkError::IoWithPath {
                        path: shard.path.clone(),
                        source,
                    }
                ));
                continue;
            }
            if let Err(err) = crate::engine::fs_utils::sync_parent_dir(&shard.path) {
                errors.push(format!(
                    "sync parent of {} failed: {err}",
                    shard.path.display()
                ));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::Other(errors.join("; ")))
    }
}

fn combine_transaction_failure(
    operation_error: TsinkError,
    cleanup: Result<()>,
    accounting: Result<()>,
) -> TombstonePersistenceError {
    if cleanup.is_ok() && accounting.is_ok() {
        return TombstonePersistenceError::definitively_clean(operation_error);
    }
    let mut errors = vec![format!("transaction failed: {operation_error}")];
    if let Err(err) = cleanup {
        errors.push(format!("cleanup failed: {err}"));
    }
    if let Err(err) = accounting {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    TombstonePersistenceError::indeterminate(TsinkError::Other(errors.join("; ")))
}

fn rollback_uncoordinated_tombstone_transaction(
    plans: &[PreparedTombstoneStoreUpdate],
    owned_paths: &HashSet<PathBuf>,
    reservation: Option<crate::disk_budget::DiskReservation>,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    operation_error: TsinkError,
) -> TombstonePersistenceError {
    combine_transaction_failure(
        operation_error,
        remove_owned_prepared_tombstone_shards(plans, owned_paths),
        settle_tombstone_transaction_reservation(reservation, budget),
    )
}

fn current_manifest_matches(
    lane: &TombstoneLane,
    previous: Option<&[u8]>,
    candidate: Option<&[u8]>,
) -> Result<(bool, bool)> {
    let recorded_len_bound = previous
        .map_or(0, <[u8]>::len)
        .max(candidate.map_or(0, <[u8]>::len));
    let current = read_optional_regular_file_bounded(&lane.manifest_path, recorded_len_bound)?;
    let previous_matches = current.as_deref() == previous;
    let candidate_matches = current.as_deref() == candidate;
    if !previous_matches && !candidate_matches {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone lane {:?} is neither its recorded previous nor candidate manifest",
            lane.role
        )));
    }
    Ok((previous_matches, candidate_matches))
}

fn synchronize_manifest_parent_for_record(
    lane: &TombstoneLane,
    record_lane: &TombstoneTransactionLaneRecord,
) -> Result<()> {
    validate_tombstone_lane_namespace(lane)?;
    let parent = lane.manifest_path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "tombstone manifest has no parent: {}",
            lane.manifest_path.display()
        ))
    })?;
    match std::fs::symlink_metadata(parent) {
        Ok(metadata)
            if metadata.file_type().is_dir()
                && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
        {
            crate::engine::fs_utils::sync_dir(parent)
        }
        Ok(_) => Err(TsinkError::DataCorruption(format!(
            "tombstone lane root must remain a directory and may not be a symlink: {}",
            parent.display()
        ))),
        Err(err)
            if err.kind() == std::io::ErrorKind::NotFound
                && record_lane.previous_manifest.is_none()
                && record_lane.candidate_manifest.is_none() =>
        {
            Ok(())
        }
        Err(source) => Err(TsinkError::IoWithPath {
            path: parent.to_path_buf(),
            source,
        }),
    }
}

fn prove_candidate_manifest_set_durable(
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
) -> Result<()> {
    for (lane, record_lane) in lanes.iter().zip(&record.lanes) {
        synchronize_manifest_parent_for_record(lane, record_lane)?;
        let (_, candidate_matches) = current_manifest_matches(
            lane,
            record_lane.previous_manifest.as_deref(),
            record_lane.candidate_manifest.as_deref(),
        )?;
        if !candidate_matches {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone lane {:?} is not durably at its committed candidate",
                lane.role
            )));
        }
    }
    Ok(())
}

fn candidate_shard_names(manifest: Option<&TombstoneStoreManifestV2>) -> HashSet<String> {
    manifest
        .into_iter()
        .flat_map(|manifest| manifest.shards.iter().flatten().cloned())
        .collect()
}

fn validate_candidate_shards(
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
) -> Result<()> {
    for (lane, record_lane) in lanes.iter().zip(&record.lanes) {
        for shard in &record_lane.candidate_shards {
            let shard_path = tombstone_shards_dir(&lane.manifest_path).join(&shard.file_name);
            let metadata = std::fs::symlink_metadata(&shard_path).map_err(|source| {
                TsinkError::IoWithPath {
                    path: shard_path.clone(),
                    source,
                }
            })?;
            if !metadata.file_type().is_file()
                || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || metadata.len() != shard.logical_bytes
                || shard.logical_bytes > MAX_TOMBSTONE_SHARD_BYTES as u64
            {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate tombstone shard length/type mismatch: {}",
                    shard_path.display()
                )));
            }
            let limit = usize::try_from(shard.logical_bytes).map_err(|_| {
                TsinkError::DataCorruption(format!(
                    "candidate tombstone shard length is unsupported: {}",
                    shard_path.display()
                ))
            })?;
            let bytes =
                read_optional_regular_file_bounded(&shard_path, limit)?.ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "candidate tombstone shard is missing: {}",
                        shard_path.display()
                    ))
                })?;
            if xxhash_rust::xxh64::xxh64(&bytes, 0) != shard.xxh64 {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate tombstone shard digest mismatch: {}",
                    shard_path.display()
                )));
            }
            preflight_shard_bytes(&bytes, Some(usize::from(shard.shard_index)))?;
        }
    }
    Ok(())
}

fn remove_record_candidate_shards(
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
) -> Result<()> {
    let mut errors = Vec::new();
    for (lane, record_lane) in lanes.iter().zip(&record.lanes).rev() {
        let previous = validated_manifest_image(record_lane.previous_manifest.as_deref(), false)?;
        let candidate = validated_manifest_image(record_lane.candidate_manifest.as_deref(), true)?;
        let previous_names = candidate_shard_names(previous.as_ref());
        for file_name in candidate_shard_names(candidate.as_ref()) {
            if previous_names.contains(&file_name) {
                continue;
            }
            let shard_index = file_name
                .strip_prefix("shard-")
                .and_then(|name| name.get(..3))
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "invalid candidate tombstone shard file name {file_name:?}"
                    ))
                })?;
            validate_tombstone_shard_file_name(shard_index, &file_name)?;
            let path = tombstone_shards_dir(&lane.manifest_path).join(&file_name);
            let fingerprint = record_lane
                .candidate_shards
                .iter()
                .find(|shard| shard.file_name == file_name)
                .ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "missing candidate shard fingerprint for {}",
                        path.display()
                    ))
                })?;
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    errors.push(format!(
                        "inspect {} failed: {}",
                        path.display(),
                        TsinkError::IoWithPath {
                            path: path.clone(),
                            source,
                        }
                    ));
                    continue;
                }
            };
            if !metadata.file_type().is_file()
                || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate tombstone shard must remain a regular file and may not be a symlink: {}",
                    path.display()
                )));
            }
            if metadata.len() != fingerprint.logical_bytes {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate tombstone shard length changed before cleanup: {}",
                    path.display()
                )));
            }
            let limit = usize::try_from(fingerprint.logical_bytes).map_err(|_| {
                TsinkError::DataCorruption(format!(
                    "candidate tombstone shard length is unsupported: {}",
                    path.display()
                ))
            })?;
            let bytes = read_optional_regular_file_bounded(&path, limit)?.ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "candidate tombstone shard disappeared before cleanup: {}",
                    path.display()
                ))
            })?;
            if xxhash_rust::xxh64::xxh64(&bytes, 0) != fingerprint.xxh64 {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate tombstone shard digest changed before cleanup: {}",
                    path.display()
                )));
            }
            // Structurally validate the exact bytes already fingerprinted. This avoids a second
            // full-file allocation/read during recovery cleanup.
            preflight_shard_bytes(&bytes, Some(shard_index))?;
            if let Err(source) = std::fs::remove_file(&path) {
                errors.push(format!(
                    "remove {} failed: {}",
                    path.display(),
                    TsinkError::IoWithPath {
                        path: path.clone(),
                        source,
                    }
                ));
                continue;
            }
            if let Err(err) = crate::engine::fs_utils::sync_parent_dir(&path) {
                errors.push(format!("sync parent of {} failed: {err}", path.display()));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::Other(errors.join("; ")))
    }
}

fn recover_prepared_tombstone_transaction(
    data_path: &Path,
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
) -> Result<()> {
    for (lane, record_lane) in lanes.iter().zip(&record.lanes) {
        let (previous_matches, _) = current_manifest_matches(
            lane,
            record_lane.previous_manifest.as_deref(),
            record_lane.candidate_manifest.as_deref(),
        )?;
        if !previous_matches {
            return Err(TsinkError::DataCorruption(format!(
                "Prepared tombstone transaction unexpectedly mutated lane {:?}",
                lane.role
            )));
        }
    }
    remove_record_candidate_shards(lanes, record)?;
    remove_owned_regular_file_and_sync_parent_budgeted(
        &tombstone_transaction_path(data_path),
        None,
        crate::DiskCategory::Tombstones,
    )?;
    Ok(())
}

fn publish_manifest_image(path: &Path, candidate: Option<&[u8]>) -> Result<()> {
    match candidate {
        Some(candidate) => write_file_atomically_and_sync_parent(path, candidate),
        None => remove_owned_regular_file_and_sync_parent_budgeted(
            path,
            None,
            crate::DiskCategory::Tombstones,
        )
        .map(|_| ()),
    }
}

fn recovery_manifest_admission_bytes(
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
    budget: &Arc<crate::LocalDiskBudget>,
) -> Result<u64> {
    let mut bytes = 0u64;
    let mut governed = Vec::new();
    for (lane, record_lane) in lanes.iter().zip(&record.lanes) {
        let (_, candidate_matches) = current_manifest_matches(
            lane,
            record_lane.previous_manifest.as_deref(),
            record_lane.candidate_manifest.as_deref(),
        )?;
        if candidate_matches || record_lane.candidate_manifest.is_none() {
            continue;
        }
        if budget.governs_entry(&lane.manifest_path)? {
            budget.validate_managed_file_path(&lane.manifest_path)?;
            governed.push(lane.manifest_path.clone());
            checked_add_payload_bytes(
                &mut bytes,
                record_lane.candidate_manifest.as_ref().map_or(0, Vec::len),
                "recovery tombstone manifest",
            )?;
        }
    }
    let allowance = budget.snapshot_restore_entry_staging_allowance_bytes()?;
    let target_count = u64::try_from(governed.len()).map_err(|_| {
        TsinkError::Other("tombstone recovery target count exceeds u64".to_string())
    })?;
    let missing = budget.missing_managed_parent_directory_count(&governed)?;
    bytes
        .checked_add(
            target_count
                .checked_add(missing)
                .and_then(|count| count.checked_mul(allowance))
                .ok_or_else(|| {
                    TsinkError::Other("tombstone recovery admission overflow".to_string())
                })?,
        )
        .ok_or_else(|| TsinkError::Other("tombstone recovery byte overflow".to_string()))
}

fn cleanup_committed_record_shards(
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
) -> Vec<String> {
    let mut errors = Vec::new();
    for (lane, record_lane) in lanes.iter().zip(&record.lanes) {
        let previous =
            match validated_manifest_image(record_lane.previous_manifest.as_deref(), false) {
                Ok(previous) => previous,
                Err(err) => {
                    errors.push(format!("{:?}: {err}", lane.role));
                    continue;
                }
            };
        let candidate =
            match validated_manifest_image(record_lane.candidate_manifest.as_deref(), true) {
                Ok(candidate) => candidate,
                Err(err) => {
                    errors.push(format!("{:?}: {err}", lane.role));
                    continue;
                }
            };
        if let Some(previous) = previous {
            let candidate_shards = candidate
                .as_ref()
                .map(|manifest| manifest.shards.as_slice())
                .unwrap_or(&[]);
            if let Err(err) = cleanup_replaced_shards(
                &lane.manifest_path,
                &previous.shards,
                candidate_shards,
                None,
            ) {
                errors.push(format!("{:?}: {err}", lane.role));
            }
        }
    }
    errors
}

fn recover_committing_tombstone_transaction(
    data_path: &Path,
    lanes: &[TombstoneLane],
    record: &TombstoneTransactionRecord,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let candidate_matches = lanes
        .iter()
        .zip(&record.lanes)
        .map(|(lane, record_lane)| {
            synchronize_manifest_parent_for_record(lane, record_lane)?;
            current_manifest_matches(
                lane,
                record_lane.previous_manifest.as_deref(),
                record_lane.candidate_manifest.as_deref(),
            )
            .map(|(_, candidate_matches)| candidate_matches)
        })
        .collect::<Result<Vec<_>>>()?;
    validate_candidate_shards(lanes, record)?;
    let peak = budget
        .map(|budget| recovery_manifest_admission_bytes(lanes, record, budget))
        .transpose()?
        .unwrap_or(0);
    let mut reservation = if peak == 0 {
        None
    } else {
        budget
            .map(|budget| {
                budget.reserve(
                    crate::DiskCategory::Tombstones,
                    peak,
                    crate::DiskReservationKind::Recovery,
                )
            })
            .transpose()?
    };

    for ((lane, record_lane), candidate_matches) in
        lanes.iter().zip(&record.lanes).zip(candidate_matches)
    {
        if !candidate_matches {
            if let Err(err) = publish_manifest_image(
                &lane.manifest_path,
                record_lane.candidate_manifest.as_deref(),
            ) {
                let accounting =
                    settle_tombstone_transaction_reservation(reservation.take(), budget);
                return match accounting {
                    Ok(()) => Err(err),
                    Err(accounting_err) => Err(TsinkError::Other(format!(
                        "tombstone recovery publication failed: {err}; disk reconciliation failed: {accounting_err}"
                    ))),
                };
            }
        }
    }
    if let Err(err) = prove_candidate_manifest_set_durable(lanes, record) {
        let accounting = settle_tombstone_transaction_reservation(reservation.take(), budget);
        return match accounting {
            Ok(()) => Err(err),
            Err(accounting_err) => Err(TsinkError::Other(format!(
                "tombstone recovery durability proof failed: {err}; disk reconciliation failed: {accounting_err}"
            ))),
        };
    }
    settle_tombstone_transaction_reservation(reservation.take(), budget)?;
    remove_owned_regular_file_and_sync_parent_budgeted(
        &tombstone_transaction_path(data_path),
        None,
        crate::DiskCategory::Tombstones,
    )?;

    let mut cleanup_errors = cleanup_committed_record_shards(lanes, record);
    if let Some(budget) = budget {
        if let Err(err) = budget.reconcile_when_idle() {
            cleanup_errors.push(format!("disk reconciliation: {err}"));
        }
    }
    if !cleanup_errors.is_empty() {
        tracing::warn!(
            errors = %cleanup_errors.join("; "),
            "recovered tombstone transaction left conservatively-accounted cleanup work"
        );
    }
    Ok(())
}

fn tombstone_recovery_memory_upper_bound(
    record: &TombstoneTransactionRecord,
    encoded_record_bytes: usize,
) -> Result<usize> {
    let retained_record = encoded_record_bytes
        .saturating_mul(8)
        .saturating_add(16 * 1024);
    let legacy_decode_peak = record
        .lanes
        .iter()
        .filter_map(|lane| lane.previous_manifest.as_deref())
        .filter(|bytes| !bytes.starts_with(TOMBSTONE_STORE_MAGIC))
        .map(|bytes| bytes.len().saturating_mul(64).saturating_add(4096))
        .max()
        .unwrap_or(0);
    let candidate_shard_peak = record
        .lanes
        .iter()
        .flat_map(|lane| &lane.candidate_shards)
        .map(|shard| {
            usize::try_from(shard.logical_bytes)
                .unwrap_or(usize::MAX)
                .saturating_mul(2)
                .saturating_add(4096)
        })
        .max()
        .unwrap_or(0);
    let current_manifest_peak = record
        .lanes
        .iter()
        .map(|lane| {
            lane.previous_manifest
                .as_ref()
                .map_or(0, Vec::len)
                .max(lane.candidate_manifest.as_ref().map_or(0, Vec::len))
                .saturating_mul(2)
                .saturating_add(4096)
        })
        .max()
        .unwrap_or(0);
    Ok(retained_record.saturating_add(
        legacy_decode_peak
            .max(candidate_shard_peak)
            .max(current_manifest_peak),
    ))
}

/// Preloads the complete authoritative map named by a durable `Committing` coordinator.
///
/// This is intentionally read-only. Callers retain the returned map and its reservation while
/// rolling the coordinator forward, so no manifest mutation can expose a committed candidate
/// whose live in-memory visibility can still fail allocation or shard I/O afterward.
pub(crate) fn prepare_committed_tombstone_reload_with_memory_admission(
    data_path: &Path,
    lanes: &[TombstoneLane],
    item_limit: usize,
    byte_limit: u64,
    base_work_bytes: u64,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<Option<PreparedCommittedTombstoneReload>> {
    let data_path = resolve_trusted_namespace_root(data_path)?;
    let lanes = normalize_tombstone_lanes(lanes)?;
    let data_path = data_path.as_path();
    let lanes = lanes.as_slice();
    validate_tombstone_lanes(lanes)?;
    validate_tombstone_coordinator_namespace(data_path)?;

    let coordinator_path = tombstone_transaction_path(data_path);
    let coordinator_metadata = match std::fs::symlink_metadata(&coordinator_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: coordinator_path,
                source,
            })
        }
    };
    if !coordinator_metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&coordinator_metadata)
        || coordinator_metadata.len() > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone transaction coordinator has invalid type or length: {}",
            coordinator_path.display()
        )));
    }
    let encoded_record_bytes = usize::try_from(coordinator_metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(
            "tombstone transaction coordinator length is unsupported".to_string(),
        )
    })?;
    let record_decode_peak = encoded_record_bytes
        .saturating_mul(8)
        .saturating_add(16 * 1024);
    admit_memory(record_decode_peak)?;
    let encoded_record = read_required_regular_file_bounded_exact(
        &coordinator_path,
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
        encoded_record_bytes,
    )?;
    let record = decode_tombstone_transaction_record(&encoded_record)?;
    let recovery_memory_upper_bound =
        tombstone_recovery_memory_upper_bound(&record, encoded_record_bytes)?;
    admit_memory(recovery_memory_upper_bound)?;
    validate_transaction_record_against_lanes(&record, lanes)?;
    if record.phase != TombstoneTransactionPhase::Committing {
        return Ok(None);
    }

    // This helper preloads the candidate and then the caller runs the coordinator recovery while
    // retaining that map. Bound cumulative finite-pass work, not only the largest live buffer:
    // coordinator decoding occurs in both phases and recovery inspects its recorded manifests.
    let mut work_bytes = base_work_bytes.saturating_add(
        u64::try_from(
            record_decode_peak
                .saturating_mul(2)
                .saturating_add(recovery_memory_upper_bound),
        )
        .unwrap_or(u64::MAX),
    );
    if work_bytes > byte_limit {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "committed tombstone reload",
            limit: byte_limit,
            required: work_bytes,
        });
    }

    let mut merged = TombstoneMap::new();
    // Count both coordinator reads, both candidate-shard inspections, and the fixed per-lane
    // manifest/durability operations before opening the first payload. Series entries and ranges
    // are added from each bounded shard preflight below; namespace enumeration is preflighted by
    // the caller against the same totals before recovery mutates anything.
    let candidate_shard_count = record
        .lanes
        .iter()
        .try_fold(0usize, |total, lane| {
            total.checked_add(lane.candidate_shards.len())
        })
        .ok_or_else(|| {
            TsinkError::DataCorruption(
                "committed tombstone reload work-item count overflow".to_string(),
            )
        })?;
    let mut work_items = 2usize
        .saturating_add(candidate_shard_count.saturating_mul(2))
        .saturating_add(record.lanes.len().saturating_mul(4));
    if work_items > item_limit {
        return Err(TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "committed tombstone reload",
            item_limit,
            byte_limit,
            selected_items: work_items,
            selected_bytes: work_bytes,
        });
    }
    for (lane, record_lane) in lanes.iter().zip(&record.lanes) {
        for shard in &record_lane.candidate_shards {
            let shard_index = usize::from(shard.shard_index);
            let expected_len = usize::try_from(shard.logical_bytes).map_err(|_| {
                TsinkError::DataCorruption(format!(
                    "candidate tombstone shard length is unsupported: {}",
                    shard.file_name
                ))
            })?;
            let shard_work_bytes = u64::try_from(expected_len)
                .unwrap_or(u64::MAX)
                // One decoded preload plus the recovery phase's second digest read.
                .saturating_mul(65)
                .saturating_add(8 * 1024);
            work_bytes = work_bytes.saturating_add(shard_work_bytes);
            if work_bytes > byte_limit {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: "committed tombstone reload",
                    limit: byte_limit,
                    required: work_bytes,
                });
            }
            let shard_path = tombstone_shards_dir(&lane.manifest_path).join(&shard.file_name);
            validate_tombstone_shard_path(&shard_path)?;
            let merged_bytes = tombstone_map_allocation_bytes(&merged);
            admit_memory(
                recovery_memory_upper_bound
                    .saturating_add(merged_bytes)
                    .saturating_add(expected_len.saturating_mul(64))
                    .saturating_add(4096),
            )?;
            let bytes = read_required_regular_file_bounded_exact(
                &shard_path,
                MAX_TOMBSTONE_SHARD_BYTES,
                expected_len,
            )?;
            if xxhash_rust::xxh64::xxh64(&bytes, 0) != shard.xxh64 {
                return Err(TsinkError::DataCorruption(format!(
                    "candidate tombstone shard digest mismatch: {}",
                    shard_path.display()
                )));
            }
            let preflight = preflight_shard_bytes(&bytes, Some(shard_index))?;
            let shard_work_items = preflight
                .entry_count
                .checked_add(preflight.total_range_count)
                .ok_or_else(|| {
                    TsinkError::DataCorruption(
                        "committed tombstone reload work-item count overflow".to_string(),
                    )
                })?;
            work_items = work_items.checked_add(shard_work_items).ok_or_else(|| {
                TsinkError::DataCorruption(
                    "committed tombstone reload work-item count overflow".to_string(),
                )
            })?;
            if work_items > item_limit {
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation: "committed tombstone reload",
                    item_limit,
                    byte_limit,
                    selected_items: work_items,
                    selected_bytes: work_bytes,
                });
            }
            admit_memory(
                recovery_memory_upper_bound
                    .saturating_add(merged_bytes)
                    .saturating_add(preflight.decoded_memory_upper_bound(bytes.len())),
            )?;
            let loaded = decode_shard_map(&bytes, shard_index)?;
            drop(bytes);
            let loaded_bytes = tombstone_map_allocation_bytes(&loaded);
            admit_memory(
                recovery_memory_upper_bound
                    .saturating_add(merged_bytes.saturating_mul(2))
                    .saturating_add(loaded_bytes.saturating_mul(2))
                    .saturating_add(4096),
            )?;
            for (series_id, ranges) in loaded {
                for range in ranges {
                    merge_tombstone_range(merged.entry(series_id).or_default(), range);
                }
            }
            admit_memory(
                recovery_memory_upper_bound.saturating_add(tombstone_map_allocation_bytes(&merged)),
            )?;
        }
    }

    Ok(Some(PreparedCommittedTombstoneReload {
        tombstones: merged,
        recovery_memory_upper_bound,
        work_items,
        work_bytes,
    }))
}

pub(crate) fn recover_tombstone_transaction_with_memory_admission(
    data_path: &Path,
    lanes: &[TombstoneLane],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<TombstoneRecoveryOutcome> {
    let data_path = resolve_trusted_namespace_root(data_path)?;
    let lanes = normalize_tombstone_lanes(lanes)?;
    let data_path = data_path.as_path();
    let lanes = lanes.as_slice();
    validate_tombstone_lanes(lanes)?;
    validate_tombstone_coordinator_namespace(data_path)?;
    cleanup_tombstone_atomic_temps_before_recovery(data_path, lanes, budget, &mut admit_memory)?;
    validate_tombstone_coordinator_namespace(data_path)?;
    validate_tombstone_lanes(lanes)?;
    let coordinator_dir = tombstone_transaction_dir(data_path);
    match std::fs::symlink_metadata(&coordinator_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(TombstoneRecoveryOutcome::NoTransaction)
        }
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: coordinator_dir,
                source,
            })
        }
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone transaction coordinator namespace must be a directory and may not be a symlink: {}",
                coordinator_dir.display()
            )))
        }
        Ok(_) => {}
    }
    // A prior atomic writer may have returned after making a rename merely visible. Synchronize
    // the coordinator directory before interpreting either phase; failure authorizes no lane I/O.
    crate::engine::fs_utils::sync_dir(&coordinator_dir)?;
    let coordinator_path = tombstone_transaction_path(data_path);
    let coordinator_metadata = match std::fs::symlink_metadata(&coordinator_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(TombstoneRecoveryOutcome::NoTransaction)
        }
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: coordinator_path,
                source,
            })
        }
    };
    if !coordinator_metadata.file_type().is_file()
        || crate::engine::fs_utils::is_link_or_reparse_point(&coordinator_metadata)
        || coordinator_metadata.len() > MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone transaction coordinator has invalid type or length: {}",
            coordinator_path.display()
        )));
    }
    let encoded_record_bytes = usize::try_from(coordinator_metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(
            "tombstone transaction coordinator length is unsupported".to_string(),
        )
    })?;
    admit_memory(
        encoded_record_bytes
            .saturating_mul(8)
            .saturating_add(16 * 1024),
    )?;
    let encoded_record = read_required_regular_file_bounded_exact(
        &coordinator_path,
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
        encoded_record_bytes,
    )?;
    let record = decode_tombstone_transaction_record(&encoded_record)?;
    admit_memory(tombstone_recovery_memory_upper_bound(
        &record,
        encoded_record_bytes,
    )?)?;
    validate_transaction_record_against_lanes(&record, lanes)?;
    match record.phase {
        TombstoneTransactionPhase::Prepared => {
            recover_prepared_tombstone_transaction(data_path, lanes, &record)?;
            if let Some(budget) = budget {
                budget.reconcile_when_idle()?;
            }
            Ok(TombstoneRecoveryOutcome::RolledBackPrepared)
        }
        TombstoneTransactionPhase::Committing => {
            recover_committing_tombstone_transaction(data_path, lanes, &record, budget)?;
            Ok(TombstoneRecoveryOutcome::RolledForwardCommitted)
        }
    }
}

#[cfg(test)]
pub(crate) fn recover_tombstone_transaction(
    data_path: &Path,
    lanes: &[TombstoneLane],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<TombstoneRecoveryOutcome> {
    recover_tombstone_transaction_with_memory_admission(data_path, lanes, budget, |_| Ok(()))
}

fn persist_tombstone_plans_transactionally(
    data_path: &Path,
    lanes: &[TombstoneLane],
    plans: Vec<PreparedTombstoneStoreUpdate>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> TombstonePersistenceResult<()> {
    validate_tombstone_coordinator_namespace(data_path)?;
    // Deep transaction records clone every manifest image, identity path, and shard name from
    // the retained plans. Admit that clone before constructing it.
    let plans_retained = plans.iter().fold(0usize, |total, plan| {
        total.saturating_add(prepared_tombstone_plan_retained_bytes(plan))
    });
    let record_upper = transaction_record_from_plans_retained_upper_bound(&plans);
    admit_memory(plans_retained.saturating_add(record_upper))?;
    let mut prepared_record =
        transaction_record_from_plans(&plans, TombstoneTransactionPhase::Prepared);
    validate_transaction_record_against_lanes(&prepared_record, lanes)?;
    let prepared_record_retained = transaction_record_retained_bytes(&prepared_record);
    let prepared_payload = encode_tombstone_transaction_record_with_memory_admission(
        &prepared_record,
        plans_retained.saturating_add(prepared_record_retained),
        &mut admit_memory,
    )?;
    admit_memory(
        plans_retained
            .saturating_add(prepared_record_retained)
            .saturating_add(prepared_payload.capacity())
            .saturating_add(record_upper)
            .saturating_add(16 * 1024),
    )?;
    let expected_prepared_record = prepared_record.clone();
    prepared_record.phase = TombstoneTransactionPhase::Committing;
    let committing_payload = encode_tombstone_transaction_record_with_memory_admission(
        &prepared_record,
        plans_retained
            .saturating_add(prepared_record_retained.saturating_mul(2))
            .saturating_add(prepared_payload.capacity()),
        &mut admit_memory,
    )?;
    let retained_transaction_bytes = plans_retained
        .saturating_add(prepared_record_retained.saturating_mul(2))
        .saturating_add(prepared_payload.capacity())
        .saturating_add(committing_payload.capacity());
    let target_path_staging = tombstone_transaction_path_staging_upper_bound(data_path, &plans);
    let owned_shard_paths_retained = tombstone_owned_shard_paths_retained_upper_bound(&plans);
    admit_memory(
        retained_transaction_bytes
            .saturating_add(target_path_staging)
            .saturating_add(owned_shard_paths_retained)
            .saturating_add(TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK),
    )?;
    let coordinator_peak = prepared_payload
        .len()
        .checked_add(committing_payload.len())
        .ok_or_else(|| {
            TombstonePersistenceError::definitively_clean(TsinkError::Other(
                "tombstone coordinator peak byte count overflow".to_string(),
            ))
        })?;
    let peak_bytes = local_disk_budget
        .map(|budget| {
            tombstone_transaction_budget_bytes(data_path, &plans, coordinator_peak, budget)
        })
        .transpose()?
        .unwrap_or(0);
    let mut reservation = if peak_bytes == 0 {
        None
    } else {
        local_disk_budget
            .map(|budget| {
                budget.reserve(
                    crate::DiskCategory::Tombstones,
                    peak_bytes,
                    reservation_kind,
                )
            })
            .transpose()?
    };
    let owned_shard_count = plans.iter().fold(0usize, |total, plan| {
        total.saturating_add(plan.new_shards.len())
    });
    let mut owned_shard_paths = HashSet::with_capacity(owned_shard_count);

    if let Err(err) =
        prepare_tombstone_transaction_directories(data_path, &plans, local_disk_budget)
    {
        return Err(rollback_uncoordinated_tombstone_transaction(
            &plans,
            &owned_shard_paths,
            reservation,
            local_disk_budget,
            err,
        ));
    }
    if let Err(err) = validate_tombstone_coordinator_namespace(data_path)
        .and_then(|()| validate_tombstone_lanes(lanes))
    {
        return Err(rollback_uncoordinated_tombstone_transaction(
            &plans,
            &owned_shard_paths,
            reservation,
            local_disk_budget,
            err,
        ));
    }
    for plan in &plans {
        for shard in &plan.new_shards {
            let mut file = match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&shard.path)
            {
                Ok(file) => file,
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(rollback_uncoordinated_tombstone_transaction(
                        &plans,
                        &owned_shard_paths,
                        reservation,
                        local_disk_budget,
                        TsinkError::DataCorruption(format!(
                            "candidate tombstone shard unexpectedly already exists: {}",
                            shard.path.display()
                        )),
                    ))
                }
                Err(source) => {
                    return Err(rollback_uncoordinated_tombstone_transaction(
                        &plans,
                        &owned_shard_paths,
                        reservation,
                        local_disk_budget,
                        TsinkError::IoWithPath {
                            path: shard.path.clone(),
                            source,
                        },
                    ))
                }
            };
            owned_shard_paths.insert(shard.path.clone());
            let write_result = (|| -> Result<()> {
                file.write_all(&shard.payload)
                    .map_err(|source| TsinkError::IoWithPath {
                        path: shard.path.clone(),
                        source,
                    })?;
                file.flush().map_err(|source| TsinkError::IoWithPath {
                    path: shard.path.clone(),
                    source,
                })?;
                file.sync_all().map_err(|source| TsinkError::IoWithPath {
                    path: shard.path.clone(),
                    source,
                })?;
                crate::engine::fs_utils::sync_parent_dir(&shard.path)
            })();
            drop(file);
            if let Err(err) = write_result {
                return Err(rollback_uncoordinated_tombstone_transaction(
                    &plans,
                    &owned_shard_paths,
                    reservation,
                    local_disk_budget,
                    err,
                ));
            }
        }
    }

    let coordinator_path = tombstone_transaction_path(data_path);
    if let Err(err) = write_file_atomically_and_sync_parent(&coordinator_path, &prepared_payload) {
        // The atomic helper may report an error after rename. Only a successful parent sync probe
        // makes the visible presence/absence durable enough to decide whether candidate shards
        // can be removed. A failed probe preserves both the record and every candidate.
        if let Err(sync_err) = crate::engine::fs_utils::sync_parent_dir(&coordinator_path) {
            let accounting =
                settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
            let mut errors = vec![
                format!("Prepared coordinator publication failed: {err}"),
                format!("coordinator parent durability probe failed: {sync_err}"),
            ];
            if let Err(accounting_err) = accounting {
                errors.push(format!("disk reconciliation failed: {accounting_err}"));
            }
            return Err(TombstonePersistenceError::indeterminate(TsinkError::Other(
                errors.join("; "),
            )));
        }
        let cleanup = match read_tombstone_transaction_record(data_path) {
            Ok(None) => remove_owned_prepared_tombstone_shards(&plans, &owned_shard_paths),
            Ok(Some(record)) if record == expected_prepared_record => {
                recover_prepared_tombstone_transaction(data_path, lanes, &record)
            }
            Ok(Some(_)) => Err(TsinkError::DataCorruption(
                "ambiguous tombstone coordinator publication produced an unexpected record"
                    .to_string(),
            )),
            Err(read_err) => Err(read_err),
        };
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        return Err(combine_transaction_failure(err, cleanup, accounting));
    }

    let precommit_validation = (|| -> Result<()> {
        validate_tombstone_coordinator_namespace(data_path)?;
        validate_tombstone_lanes(lanes)?;
        validate_candidate_shards(lanes, &expected_prepared_record)?;
        for (lane, record_lane) in lanes.iter().zip(&expected_prepared_record.lanes) {
            let (previous_matches, _) = current_manifest_matches(
                lane,
                record_lane.previous_manifest.as_deref(),
                record_lane.candidate_manifest.as_deref(),
            )?;
            if !previous_matches {
                return Err(TsinkError::DataCorruption(format!(
                    "tombstone lane {:?} changed while the coordinator was Prepared",
                    lane.role
                )));
            }
        }
        Ok(())
    })();
    if let Err(err) = precommit_validation {
        let cleanup =
            recover_prepared_tombstone_transaction(data_path, lanes, &expected_prepared_record);
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        return Err(combine_transaction_failure(err, cleanup, accounting));
    }

    #[cfg(test)]
    if let Err(err) =
        invoke_tombstone_transaction_hook(TombstoneTransactionTestPoint::BeforeCommitDecision)
    {
        let cleanup =
            recover_prepared_tombstone_transaction(data_path, lanes, &expected_prepared_record);
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        return Err(combine_transaction_failure(err, cleanup, accounting));
    }

    // This parent-synchronized rewrite is the only commit decision. Any error is indeterminate;
    // the caller must not publish a lane manifest, even if the new bytes happen to be visible.
    if let Err(err) = write_file_atomically_and_sync_parent(&coordinator_path, &committing_payload)
    {
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        let error = match accounting {
            Ok(()) => err,
            Err(accounting_err) => TsinkError::Other(format!(
                "tombstone commit-decision publication failed: {err}; disk reconciliation failed: {accounting_err}"
            )),
        };
        return Err(TombstonePersistenceError::indeterminate(error));
    }

    // Compute-only readers cannot see this writer's local coordinator. Publish one complete
    // shared manifest as their durable visibility anchor before any local/secondary manifest.
    // Every delete candidate is a monotonic union of its predecessor plus new ranges, so this
    // single shared image is sufficient to hide every acknowledged delete while lagging lanes
    // are rolled forward by the writer's coordinator recovery.
    let shared_visibility_anchor = lanes.iter().position(|lane| lane.role.is_shared_remote());
    let mut shared_visibility_anchor_durable = shared_visibility_anchor.is_none();

    #[cfg(test)]
    if let Err(err) =
        invoke_tombstone_transaction_hook(TombstoneTransactionTestPoint::AmbiguousCommitDecision)
    {
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        let error = match accounting {
            Ok(()) => err,
            Err(accounting_err) => TsinkError::Other(format!(
                "ambiguous tombstone commit-decision durability: {err}; disk reconciliation failed: {accounting_err}"
            )),
        };
        return Err(TombstonePersistenceError::indeterminate(error));
    }

    #[cfg(test)]
    if let Err(err) =
        invoke_tombstone_transaction_hook(TombstoneTransactionTestPoint::AfterCommitDecision)
    {
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        let error = match accounting {
            Ok(()) => err,
            Err(accounting_err) => TsinkError::Other(format!(
                "committed tombstone interruption: {err}; disk reconciliation failed: {accounting_err}"
            )),
        };
        return Err(if shared_visibility_anchor_durable {
            TombstonePersistenceError::committed(error)
        } else {
            TombstonePersistenceError::indeterminate(error)
        });
    }

    for publication_index in 0..plans.len() {
        let manifest_index = match shared_visibility_anchor {
            Some(anchor) if publication_index == 0 => anchor,
            Some(anchor) if publication_index <= anchor => publication_index - 1,
            _ => publication_index,
        };
        let plan = &plans[manifest_index];
        let lane = &lanes[manifest_index];
        let record_lane = &prepared_record.lanes[manifest_index];
        #[cfg(test)]
        if let Err(err) = invoke_tombstone_transaction_hook(
            TombstoneTransactionTestPoint::BeforeManifest(manifest_index),
        ) {
            let accounting =
                settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
            let error = match accounting {
                Ok(()) => err,
                Err(accounting_err) => TsinkError::Other(format!(
                    "committed tombstone interruption: {err}; disk reconciliation failed: {accounting_err}"
                )),
            };
            return Err(if shared_visibility_anchor_durable {
                TombstonePersistenceError::committed(error)
            } else {
                TombstonePersistenceError::indeterminate(error)
            });
        }
        let publication = (|| -> Result<()> {
            validate_tombstone_lane_namespace(lane)?;
            let (previous_matches, candidate_matches) = current_manifest_matches(
                lane,
                record_lane.previous_manifest.as_deref(),
                record_lane.candidate_manifest.as_deref(),
            )?;
            if candidate_matches {
                return Ok(());
            }
            if !previous_matches {
                return Err(TsinkError::DataCorruption(format!(
                    "tombstone lane {:?} changed after the durable commit decision",
                    lane.role
                )));
            }
            publish_manifest_image(&plan.path, plan.next_manifest_payload.as_deref())
        })();
        if let Err(err) = publication {
            let accounting =
                settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
            let error = match accounting {
                Ok(()) => err,
                Err(accounting_err) => TsinkError::Other(format!(
                    "committed tombstone transaction needs recovery: {err}; disk reconciliation failed: {accounting_err}"
                )),
            };
            return Err(if shared_visibility_anchor_durable {
                TombstonePersistenceError::committed(error)
            } else {
                TombstonePersistenceError::indeterminate(error)
            });
        }
        if shared_visibility_anchor == Some(manifest_index) {
            shared_visibility_anchor_durable = true;
        }
        #[cfg(test)]
        if let Err(err) = invoke_tombstone_transaction_hook(
            TombstoneTransactionTestPoint::AfterManifest(manifest_index),
        ) {
            let accounting =
                settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
            let error = match accounting {
                Ok(()) => err,
                Err(accounting_err) => TsinkError::Other(format!(
                    "committed tombstone interruption: {err}; disk reconciliation failed: {accounting_err}"
                )),
            };
            return Err(if shared_visibility_anchor_durable {
                TombstonePersistenceError::committed(error)
            } else {
                TombstonePersistenceError::indeterminate(error)
            });
        }
    }

    if let Err(err) = prove_candidate_manifest_set_durable(lanes, &prepared_record) {
        let accounting =
            settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget);
        let error = match accounting {
            Ok(()) => err,
            Err(accounting_err) => TsinkError::Other(format!(
                "committed tombstone durability proof failed: {err}; disk reconciliation failed: {accounting_err}"
            )),
        };
        return Err(TombstonePersistenceError::committed(error));
    }

    if let Err(err) =
        settle_tombstone_transaction_reservation(reservation.take(), local_disk_budget)
    {
        return Err(TombstonePersistenceError::committed(err));
    }
    if let Err(err) = remove_owned_regular_file_and_sync_parent_budgeted(
        &coordinator_path,
        None,
        crate::DiskCategory::Tombstones,
    ) {
        return Err(TombstonePersistenceError::committed(err));
    }

    let record = prepared_record;
    let mut cleanup_errors = cleanup_committed_record_shards(lanes, &record);
    if let Some(budget) = local_disk_budget {
        if let Err(err) = budget.reconcile_when_idle() {
            cleanup_errors.push(format!("disk reconciliation: {err}"));
        }
    }
    if !cleanup_errors.is_empty() {
        tracing::warn!(
            errors = %cleanup_errors.join("; "),
            "committed tombstone transaction left cleanup debt"
        );
    }
    Ok(())
}

fn persist_tombstone_updates_transactionally(
    data_path: &Path,
    lanes: &[TombstoneLane],
    normalized_updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> TombstonePersistenceResult<()> {
    if normalized_updates.is_empty() {
        return Ok(());
    }
    let data_path = resolve_trusted_namespace_root(data_path)?;
    let lanes = normalize_tombstone_lanes(lanes)?;
    let data_path = data_path.as_path();
    let lanes = lanes.as_slice();
    validate_tombstone_lanes(lanes)?;
    let _baseline_staging = tombstone_transaction_staging_memory_upper_bound_with_admission(
        lanes,
        normalized_updates,
        &mut admit_memory,
    )?;
    let update_base = tombstone_map_allocation_bytes(normalized_updates)
        .saturating_mul(4)
        .saturating_add(16 * 1024);
    let recovery = recover_tombstone_transaction_with_memory_admission(
        data_path,
        lanes,
        local_disk_budget,
        |recovery_peak| admit_memory(update_base.saturating_add(recovery_peak)),
    )
    .map_err(TombstonePersistenceError::indeterminate)?;
    if recovery.requires_authoritative_reload() {
        return Err(TombstonePersistenceError::indeterminate(TsinkError::Other(
            "recovered a committed predecessor tombstone transaction; caller must reload durable tombstones and recompute the update"
                .to_string(),
        )));
    }
    let mut plans = Vec::with_capacity(lanes.len());
    let mut completed_plan_bytes = 0usize;
    for lane in lanes {
        let retained_before_lane = update_base.saturating_add(completed_plan_bytes);
        let plan = prepare_tombstone_store_update(lane, normalized_updates, &mut |lane_peak| {
            admit_memory(retained_before_lane.saturating_add(lane_peak))
        })?;
        completed_plan_bytes =
            completed_plan_bytes.saturating_add(prepared_tombstone_plan_retained_bytes(&plan));
        admit_memory(update_base.saturating_add(completed_plan_bytes))?;
        plans.push(plan);
    }
    persist_tombstone_plans_transactionally(
        data_path,
        lanes,
        plans,
        local_disk_budget,
        crate::DiskReservationKind::Maintenance,
        |transaction_peak| admit_memory(update_base.saturating_add(transaction_peak)),
    )
}

#[cfg(test)]
pub(crate) fn persist_tombstone_snapshot_transactionally(
    data_path: &Path,
    lanes: &[TombstoneLane],
    snapshot: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> TombstonePersistenceResult<()> {
    persist_tombstone_snapshot_transactionally_with_memory_admission(
        data_path,
        lanes,
        snapshot,
        local_disk_budget,
        |_| Ok(()),
    )
}

pub(crate) fn persist_tombstone_snapshot_transactionally_with_memory_admission(
    data_path: &Path,
    lanes: &[TombstoneLane],
    snapshot: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> TombstonePersistenceResult<()> {
    let data_path = resolve_trusted_namespace_root(data_path)?;
    let lanes = normalize_tombstone_lanes(lanes)?;
    let data_path = data_path.as_path();
    let lanes = lanes.as_slice();
    validate_tombstone_lanes(lanes)?;
    admit_memory(
        tombstone_map_allocation_bytes(snapshot)
            .saturating_mul(2)
            .saturating_add(16 * 1024),
    )?;
    let mut normalized = snapshot.clone();
    normalize_tombstone_map(&mut normalized)?;
    let _baseline_staging = tombstone_transaction_staging_memory_upper_bound_with_admission(
        lanes,
        &normalized,
        &mut admit_memory,
    )?;
    let snapshot_base = tombstone_map_allocation_bytes(&normalized)
        .saturating_mul(4)
        .saturating_add(16 * 1024);
    let recovery = recover_tombstone_transaction_with_memory_admission(
        data_path,
        lanes,
        local_disk_budget,
        |recovery_peak| admit_memory(snapshot_base.saturating_add(recovery_peak)),
    )
    .map_err(TombstonePersistenceError::indeterminate)?;
    if recovery.requires_authoritative_reload() {
        return Err(TombstonePersistenceError::indeterminate(TsinkError::Other(
            "recovered a committed predecessor tombstone transaction; caller must reload durable tombstones before snapshot persistence"
                .to_string(),
        )));
    }
    let mut plans = Vec::with_capacity(lanes.len());
    let mut completed_plan_bytes = 0usize;
    for lane in lanes {
        let retained_before_lane = snapshot_base.saturating_add(completed_plan_bytes);
        let plan = prepare_tombstone_store_snapshot(lane, &normalized, &mut |lane_peak| {
            admit_memory(retained_before_lane.saturating_add(lane_peak))
        })?;
        completed_plan_bytes =
            completed_plan_bytes.saturating_add(prepared_tombstone_plan_retained_bytes(&plan));
        admit_memory(snapshot_base.saturating_add(completed_plan_bytes))?;
        plans.push(plan);
    }
    persist_tombstone_plans_transactionally(
        data_path,
        lanes,
        plans,
        local_disk_budget,
        crate::DiskReservationKind::Recovery,
        |transaction_peak| admit_memory(snapshot_base.saturating_add(transaction_peak)),
    )
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates<'a, M>(path: &Path, updates: &'a M) -> Result<()>
where
    &'a M: IntoIterator<Item = (&'a SeriesId, &'a Vec<TombstoneRange>)>,
{
    persist_tombstone_updates_with_disk_budget(path, updates, None)
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates_with_disk_budget<'a, M>(
    path: &Path,
    updates: &'a M,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()>
where
    &'a M: IntoIterator<Item = (&'a SeriesId, &'a Vec<TombstoneRange>)>,
{
    let mut normalized_updates = ordered_tombstone_map(updates);
    normalize_tombstone_map(&mut normalized_updates)?;
    let data_path = local_disk_budget
        .map(|budget| budget.root().to_path_buf())
        .unwrap_or_else(|| {
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        });
    let lanes = [TombstoneLane {
        role: TombstoneLaneRole::LocalNumeric,
        namespace_root: path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
        manifest_path: path.to_path_buf(),
    }];
    persist_tombstone_updates_transactionally(
        &data_path,
        &lanes,
        &normalized_updates,
        local_disk_budget,
        |_| Ok(()),
    )
    .map_err(TombstonePersistenceError::into_tsink_error)
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates_across_paths_with_disk_budget(
    paths: &[PathBuf],
    updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let roles = [
        TombstoneLaneRole::LocalNumeric,
        TombstoneLaneRole::LocalBlob,
        TombstoneLaneRole::HotNumeric,
        TombstoneLaneRole::HotBlob,
        TombstoneLaneRole::WarmNumeric,
        TombstoneLaneRole::WarmBlob,
        TombstoneLaneRole::ColdNumeric,
        TombstoneLaneRole::ColdBlob,
    ];
    let data_path = local_disk_budget
        .map(|budget| budget.root().to_path_buf())
        .or_else(|| {
            paths.first().and_then(|first| {
                first
                    .parent()
                    .and_then(|parent| {
                        parent
                            .ancestors()
                            .find(|candidate| paths.iter().all(|path| path.starts_with(candidate)))
                    })
                    .map(Path::to_path_buf)
            })
        })
        .unwrap_or_else(|| PathBuf::from("."));
    let lanes = paths
        .iter()
        .zip(roles)
        .map(|(path, role)| TombstoneLane {
            role,
            namespace_root: path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            manifest_path: path.clone(),
        })
        .collect::<Vec<_>>();
    persist_tombstone_updates_across_paths_with_disk_budget_outcome(
        &data_path,
        &lanes,
        updates,
        local_disk_budget,
    )
    .map_err(TombstonePersistenceError::into_tsink_error)
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates_across_paths_with_disk_budget_outcome(
    data_path: &Path,
    lanes: &[TombstoneLane],
    updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> TombstonePersistenceResult<()> {
    persist_tombstone_updates_across_paths_with_disk_budget_outcome_and_memory_admission(
        data_path,
        lanes,
        updates,
        local_disk_budget,
        |_| Ok(()),
    )
}

pub(crate) fn persist_tombstone_updates_across_paths_with_disk_budget_outcome_and_memory_admission(
    data_path: &Path,
    lanes: &[TombstoneLane],
    updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> TombstonePersistenceResult<()> {
    admit_memory(
        tombstone_map_allocation_bytes(updates)
            .saturating_mul(2)
            .saturating_add(16 * 1024),
    )?;
    let mut normalized_updates = updates.clone();
    normalize_tombstone_map(&mut normalized_updates)
        .map_err(TombstonePersistenceError::definitively_clean)?;
    persist_tombstone_updates_transactionally(
        data_path,
        lanes,
        &normalized_updates,
        local_disk_budget,
        admit_memory,
    )
}

fn is_owned_tombstone_shard_name(name: &str) -> bool {
    let Some(value) = name
        .strip_prefix("shard-")
        .and_then(|value| value.strip_suffix(".bin"))
    else {
        return false;
    };
    let Some((shard, nonce)) = value.split_once('-') else {
        return false;
    };
    shard.len() == 3
        && shard.bytes().all(|byte| byte.is_ascii_digit())
        && shard
            .parse::<usize>()
            .is_ok_and(|shard_index| shard_index < TOMBSTONE_STORE_SHARD_COUNT)
        && nonce.len() == 16
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn tombstone_cleanup_reference_set_retained_bytes(referenced: &HashSet<String>) -> usize {
    referenced
        .capacity()
        .saturating_mul(std::mem::size_of::<String>().saturating_add(16))
        .saturating_add(referenced.iter().fold(0usize, |total, value| {
            total.saturating_add(value.capacity())
        }))
        .saturating_add(TOMBSTONE_CLEANUP_ALLOCATOR_SLACK)
}

fn tombstone_cleanup_orphan_paths_retained_bytes(
    vector_capacity: usize,
    path_payload_bytes: usize,
) -> usize {
    vector_capacity
        .saturating_mul(std::mem::size_of::<PathBuf>())
        // PathBuf does not expose its allocation capacity. Twice the observed path length plus a
        // per-path allocator allowance conservatively covers normal geometric growth.
        .saturating_add(path_payload_bytes.saturating_mul(2))
        .saturating_add(vector_capacity.saturating_mul(64))
        .saturating_add(TOMBSTONE_CLEANUP_ALLOCATOR_SLACK)
}

fn tombstone_cleanup_entry_transient_bytes(directory: &Path) -> usize {
    directory
        .as_os_str()
        .len()
        .saturating_add(TOMBSTONE_CLEANUP_ENTRY_NAME_BYTES)
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<std::fs::DirEntry>())
        .saturating_add(std::mem::size_of::<PathBuf>())
        .saturating_add(std::mem::size_of::<std::ffi::OsString>())
        .saturating_add(TOMBSTONE_CLEANUP_ALLOCATOR_SLACK)
}

#[cfg(test)]
pub(crate) fn cleanup_unreferenced_tombstone_shards(
    lane: &TombstoneLane,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<u64> {
    cleanup_unreferenced_tombstone_shards_with_memory_admission(lane, local_disk_budget, |_| Ok(()))
}

/// One exact startup plan for unreferenced immutable tombstone shard finals.
///
/// The plan retains the normalized lane, stable identities for both directories involved in the
/// cleanup, the manifest fingerprint used to classify references, and only exact owned regular
/// files. Its raw executor deliberately owns no local-disk lock, reservation, or reconciliation;
/// aggregate startup cleanup supplies those responsibilities once across every category.
pub(crate) struct TombstoneFinalOrphanCleanupPlan {
    lane: TombstoneLane,
    lane_directory: PathBuf,
    lane_directory_identity: same_file::Handle,
    shards_directory: PathBuf,
    shards_directory_identity: same_file::Handle,
    manifest_fingerprint: RemoteTombstoneManifestFingerprint,
    orphan_paths: Vec<PathBuf>,
}

impl TombstoneFinalOrphanCleanupPlan {
    pub(crate) fn governed_path(&self) -> &Path {
        &self.lane.manifest_path
    }

    pub(crate) fn modeled_heap_bytes(&self) -> Result<usize> {
        let orphan_vector = self
            .orphan_paths
            .capacity()
            .checked_mul(std::mem::size_of::<PathBuf>())
            .ok_or_else(|| {
                TsinkError::Other(
                    "tombstone final-orphan plan vector capacity overflow".to_string(),
                )
            })?;
        [
            self.lane.namespace_root.capacity(),
            self.lane.manifest_path.capacity(),
            self.lane_directory.capacity(),
            self.shards_directory.capacity(),
        ]
        .into_iter()
        .chain(self.orphan_paths.iter().map(PathBuf::capacity))
        .try_fold(orphan_vector, |total, bytes| {
            total.checked_add(bytes).ok_or_else(|| {
                TsinkError::Other(
                    "tombstone final-orphan plan retained-memory overflow".to_string(),
                )
            })
        })
    }

    pub(crate) fn execution_scratch_bytes(&self) -> Result<usize> {
        let manifest_bytes =
            usize::try_from(self.manifest_fingerprint.logical_bytes).map_err(|_| {
                TsinkError::Other(
                    "tombstone final-orphan manifest length exceeds the supported range"
                        .to_string(),
                )
            })?;
        let validation_paths = 4usize
            .checked_mul(
                std::mem::size_of::<PathBuf>()
                    .checked_add(MAX_TOMBSTONE_TRANSACTION_PATH_BYTES)
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "tombstone final-orphan execution memory overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                TsinkError::Other("tombstone final-orphan execution memory overflow".to_string())
            })?;
        manifest_bytes
            .checked_add(validation_paths)
            .and_then(|bytes| bytes.checked_add(TOMBSTONE_CLEANUP_ALLOCATOR_SLACK))
            .ok_or_else(|| {
                TsinkError::Other("tombstone final-orphan execution memory overflow".to_string())
            })
    }

    /// Executes exact removals without local-disk accounting or mutation-lock acquisition.
    pub(crate) fn execute_raw(self) -> Result<u64> {
        validate_tombstone_lane_namespace(&self.lane)?;
        for (directory, identity, description) in [
            (
                &self.lane_directory,
                &self.lane_directory_identity,
                "tombstone lane directory",
            ),
            (
                &self.shards_directory,
                &self.shards_directory_identity,
                "tombstone shards directory",
            ),
        ] {
            if !crate::engine::fs_utils::path_matches_plain_directory_identity(directory, identity)?
            {
                return Err(TsinkError::DataCorruption(format!(
                    "{description} identity changed before final-orphan cleanup: {}",
                    directory.display()
                )));
            }
        }

        let expected_len = self
            .manifest_fingerprint
            .exists
            .then(|| usize::try_from(self.manifest_fingerprint.logical_bytes))
            .transpose()
            .map_err(|_| {
                TsinkError::Other(
                    "tombstone final-orphan manifest length exceeds the supported range"
                        .to_string(),
                )
            })?;
        let current_fingerprint = revalidate_remote_tombstone_manifest(&self.lane, expected_len)?;
        if current_fingerprint != self.manifest_fingerprint {
            return Err(TsinkError::DataCorruption(format!(
                "tombstone manifest changed before final-orphan cleanup: {}",
                self.lane.manifest_path.display()
            )));
        }

        let mut removed = 0u64;
        for path in self.orphan_paths {
            let file_name = path.file_name().and_then(|name| name.to_str());
            if !file_name.is_some_and(is_owned_tombstone_shard_name) {
                return Err(TsinkError::DataCorruption(format!(
                    "planned tombstone final orphan no longer has an owned shard name: {}",
                    path.display()
                )));
            }
            if remove_owned_regular_file_and_sync_parent_observed(&path)? {
                removed = removed.saturating_add(1);
            }
        }
        Ok(removed)
    }
}

#[allow(dead_code)] // Retained as the behavior-compatible standalone cleanup adapter.
pub(crate) fn cleanup_unreferenced_tombstone_shards_with_memory_admission(
    lane: &TombstoneLane,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<u64> {
    let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
    );
    let Some(plan) = plan_unreferenced_tombstone_shard_cleanup(
        lane,
        &mut namespace_budget,
        0,
        &mut admit_memory,
    )?
    else {
        return Ok(0);
    };
    let governed = local_disk_budget
        .map(|budget| budget.governs_entry(plan.governed_path()))
        .transpose()?
        .unwrap_or(false);
    let reservation = if governed {
        Some(
            local_disk_budget
                .expect("governed tombstone cleanup requires a disk budget")
                .reserve(
                    crate::DiskCategory::Tombstones,
                    0,
                    crate::DiskReservationKind::Recovery,
                )?,
        )
    } else {
        None
    };
    let cleanup_result = plan.execute_raw();
    let removed = cleanup_result.as_ref().copied().unwrap_or(0);
    let settlement_result = reservation
        .map(|reservation| reservation.commit(0, 0))
        .transpose()
        .map(|_| ());
    let reconciliation_result =
        if governed && (removed > 0 || cleanup_result.is_err() || settlement_result.is_err()) {
            local_disk_budget
                .expect("governed tombstone cleanup requires a disk budget")
                .reconcile_when_idle()
                .map(|_| ())
        } else {
            Ok(())
        };
    combine_tombstone_final_orphan_cleanup_results(
        cleanup_result,
        settlement_result,
        reconciliation_result,
        removed,
    )
}

/// Performs the complete bounded scan and every modeled-memory admission used by orphan cleanup
/// without deleting a shard. Startup uses this for all configured lanes before executing any
/// lane cleanup, so a later lane's finite-memory rejection cannot follow an earlier lane mutation.
#[allow(dead_code)] // Retained as the behavior-compatible standalone preflight adapter.
pub(crate) fn preflight_unreferenced_tombstone_shard_cleanup_memory(
    lane: &TombstoneLane,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
    );
    plan_unreferenced_tombstone_shard_cleanup(lane, &mut namespace_budget, 0, &mut admit_memory)
        .map(drop)
}

pub(crate) fn plan_unreferenced_tombstone_shard_cleanup(
    lane: &TombstoneLane,
    namespace_budget: &mut crate::engine::fs_utils::RecoveryNamespaceBudget,
    base_retained_bytes: usize,
    mut admit_memory: impl FnMut(usize) -> Result<()>,
) -> Result<Option<TombstoneFinalOrphanCleanupPlan>> {
    for path in [&lane.namespace_root, &lane.manifest_path] {
        if path.as_os_str().as_encoded_bytes().len() > MAX_TOMBSTONE_TRANSACTION_PATH_BYTES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "tombstone cleanup path exceeds the {}-byte startup bound: {}",
                MAX_TOMBSTONE_TRANSACTION_PATH_BYTES,
                path.display()
            )));
        }
    }
    let normalization_peak = 6usize
        .checked_mul(
            std::mem::size_of::<PathBuf>()
                .checked_add(MAX_TOMBSTONE_TRANSACTION_PATH_BYTES)
                .ok_or_else(|| {
                    TsinkError::Other("tombstone cleanup normalization overflow".to_string())
                })?,
        )
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<TombstoneLane>()))
        .and_then(|bytes| bytes.checked_add(base_retained_bytes))
        .ok_or_else(|| TsinkError::Other("tombstone cleanup normalization overflow".to_string()))?;
    admit_memory(normalization_peak)?;
    let normalized = normalize_tombstone_lanes(std::slice::from_ref(lane))?;
    let lane = normalized
        .into_iter()
        .next()
        .expect("normalizing one tombstone lane returns one lane");
    validate_tombstone_lanes(std::slice::from_ref(&lane))?;
    for path in [&lane.namespace_root, &lane.manifest_path] {
        if path.as_os_str().as_encoded_bytes().len() > MAX_TOMBSTONE_TRANSACTION_PATH_BYTES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "normalized tombstone cleanup path exceeds the {}-byte startup bound: {}",
                MAX_TOMBSTONE_TRANSACTION_PATH_BYTES,
                path.display()
            )));
        }
    }
    let lane_directory_source = lane.manifest_path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "tombstone manifest has no lane directory: {}",
            lane.manifest_path.display()
        ))
    })?;
    let derived_path_peak = base_retained_bytes
        .checked_add(std::mem::size_of::<TombstoneFinalOrphanCleanupPlan>())
        .and_then(|bytes| bytes.checked_add(path_retained_bytes(&lane.namespace_root)))
        .and_then(|bytes| bytes.checked_add(path_retained_bytes(&lane.manifest_path)))
        .and_then(|bytes| {
            bytes.checked_add(2usize.checked_mul(
                std::mem::size_of::<PathBuf>().checked_add(MAX_TOMBSTONE_TRANSACTION_PATH_BYTES)?,
            )?)
        })
        .ok_or_else(|| TsinkError::Other("tombstone cleanup path memory overflow".to_string()))?;
    admit_memory(derived_path_peak)?;
    let lane_directory = lane_directory_source.to_path_buf();
    let shards_directory = tombstone_shards_dir(&lane.manifest_path);
    for path in [&lane_directory, &shards_directory] {
        if path.as_os_str().as_encoded_bytes().len() > MAX_TOMBSTONE_TRANSACTION_PATH_BYTES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "derived tombstone cleanup path exceeds the {}-byte startup bound: {}",
                MAX_TOMBSTONE_TRANSACTION_PATH_BYTES,
                path.display()
            )));
        }
    }
    let discovery_path_retained = std::mem::size_of::<TombstoneFinalOrphanCleanupPlan>()
        .checked_add(lane.namespace_root.capacity())
        .and_then(|bytes| bytes.checked_add(lane.manifest_path.capacity()))
        .and_then(|bytes| bytes.checked_add(lane_directory.capacity()))
        .and_then(|bytes| bytes.checked_add(shards_directory.capacity()))
        .ok_or_else(|| TsinkError::Other("tombstone cleanup path memory overflow".to_string()))?;
    let path = &lane.manifest_path;
    let maximum_reference_set_staging = TOMBSTONE_STORE_SHARD_COUNT
        .saturating_mul(std::mem::size_of::<String>().saturating_add(80))
        .saturating_add(TOMBSTONE_CLEANUP_ALLOCATOR_SLACK);
    let manifest_bytes = read_optional_regular_file_bounded_exact_with_admission(
        path,
        MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES,
        |len| {
            let manifest_peak = len
                .checked_mul(64)
                .and_then(|bytes| bytes.checked_add(maximum_reference_set_staging))
                .and_then(|bytes| bytes.checked_add(4096))
                .and_then(|bytes| bytes.checked_add(discovery_path_retained))
                .and_then(|bytes| bytes.checked_add(base_retained_bytes))
                .ok_or_else(|| {
                    TsinkError::Other("tombstone cleanup manifest memory overflow".to_string())
                })?;
            admit_memory(manifest_peak)
        },
    )?;
    let manifest_fingerprint = remote_tombstone_manifest_fingerprint(manifest_bytes.as_deref());
    let referenced = match manifest_bytes.as_deref() {
        Some(bytes) => match load_store_manifest_from_bytes(bytes)? {
            Some(manifest) => manifest
                .shards
                .into_iter()
                .flatten()
                .collect::<HashSet<_>>(),
            None => {
                // An existing non-V2 file implies no shard references only when it is a fully
                // valid legacy V1 snapshot. Unrecognized bytes may be a damaged V2 manifest, so
                // fail closed and retain every candidate shard for explicit recovery.
                load_legacy_tombstones_from_bytes(bytes)?;
                HashSet::new()
            }
        },
        None => HashSet::new(),
    };
    drop(manifest_bytes);

    // Revalidate the complete no-follow namespace immediately before scanning. The per-entry
    // exact-file removal below repeats the final type check and cannot recurse on a type swap.
    validate_tombstone_lane_namespace(&lane)?;
    match std::fs::symlink_metadata(&shards_directory) {
        Ok(metadata)
            if metadata.file_type().is_dir()
                && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) => {}
        Ok(_) => {
            return Err(TsinkError::DataCorruption(format!(
            "tombstone shard cleanup namespace must be a directory and may not be link-like: {}",
            shards_directory.display()
        )))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: shards_directory,
                source,
            })
        }
    }
    let lane_directory_identity = crate::engine::fs_utils::capture_plain_directory_identity(
        &lane_directory,
        "tombstone final-orphan lane directory",
    )?;
    let shards_directory_identity = crate::engine::fs_utils::capture_plain_directory_identity(
        &shards_directory,
        "tombstone final-orphan shards directory",
    )?;
    let reference_set_retained = tombstone_cleanup_reference_set_retained_bytes(&referenced);
    let entry_transient = tombstone_cleanup_entry_transient_bytes(&shards_directory);
    let mut orphan_paths = Vec::new();
    let mut orphan_path_payload_bytes = 0usize;
    // Keep deletion two-phase without retaining every unrelated directory entry. Before asking
    // the OS for each DirEntry, reserve one maximum-length transient entry plus every retained
    // candidate accumulated so far. The namespace count remains bounded and every failure occurs
    // before the first removal.
    admit_memory(
        base_retained_bytes
            .checked_add(discovery_path_retained)
            .and_then(|bytes| bytes.checked_add(reference_set_retained))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::fs::ReadDir>()))
            .ok_or_else(|| {
                TsinkError::Other("tombstone cleanup scan memory overflow".to_string())
            })?,
    )?;
    let mut entries =
        std::fs::read_dir(&shards_directory).map_err(|source| TsinkError::IoWithPath {
            path: shards_directory.clone(),
            source,
        })?;
    loop {
        let orphan_retained = tombstone_cleanup_orphan_paths_retained_bytes(
            orphan_paths.capacity(),
            orphan_path_payload_bytes,
        );
        admit_memory(
            base_retained_bytes
                .checked_add(discovery_path_retained)
                .and_then(|bytes| bytes.checked_add(reference_set_retained))
                .and_then(|bytes| bytes.checked_add(orphan_retained))
                .and_then(|bytes| bytes.checked_add(entry_transient))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::fs::ReadDir>()))
                .ok_or_else(|| {
                    TsinkError::Other("tombstone cleanup entry memory overflow".to_string())
                })?,
        )?;
        let Some(entry) = entries.next() else {
            break;
        };
        namespace_budget.observe_entry(&shards_directory, "tombstone final-orphan cleanup")?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: shards_directory.clone(),
            source,
        })?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if referenced.contains(file_name) || !is_owned_tombstone_shard_name(file_name) {
            continue;
        }
        let entry_path = entry.path();
        let entry_path_len = entry_path.as_os_str().len();
        if entry_path_len > TOMBSTONE_CLEANUP_ENTRY_PATH_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "owned tombstone orphan path exceeds the {}-byte recovery bound: {}",
                TOMBSTONE_CLEANUP_ENTRY_PATH_BYTES,
                entry_path.display()
            )));
        }
        let metadata =
            std::fs::symlink_metadata(&entry_path).map_err(|source| TsinkError::IoWithPath {
                path: entry_path.clone(),
                source,
            })?;
        if !metadata.file_type().is_file()
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        {
            return Err(TsinkError::DataCorruption(format!(
                "owned tombstone orphan must be a regular file and may not be link-like: {}",
                entry_path.display()
            )));
        }
        let next_path_payload_bytes = orphan_path_payload_bytes
            .checked_add(entry_path_len)
            .ok_or_else(|| {
                TsinkError::Other("tombstone cleanup orphan path overflow".to_string())
            })?;
        let required_len = orphan_paths.len().checked_add(1).ok_or_else(|| {
            TsinkError::Other("tombstone cleanup orphan count overflow".to_string())
        })?;
        let next_vector_capacity = if required_len <= orphan_paths.capacity() {
            orphan_paths.capacity()
        } else {
            required_len
        };
        admit_memory(
            base_retained_bytes
                .checked_add(discovery_path_retained)
                .and_then(|bytes| bytes.checked_add(reference_set_retained))
                .and_then(|bytes| {
                    bytes.checked_add(tombstone_cleanup_orphan_paths_retained_bytes(
                        next_vector_capacity,
                        next_path_payload_bytes,
                    ))
                })
                .and_then(|bytes| bytes.checked_add(entry_transient))
                .ok_or_else(|| {
                    TsinkError::Other("tombstone cleanup path-plan memory overflow".to_string())
                })?,
        )?;
        if required_len > orphan_paths.capacity() {
            orphan_paths.try_reserve_exact(1).map_err(|_| {
                TsinkError::Other(
                    "unable to allocate bounded tombstone final-orphan plan".to_string(),
                )
            })?;
        }
        orphan_paths.push(entry_path);
        orphan_path_payload_bytes = next_path_payload_bytes;
    }
    drop(entries);

    for (directory, identity, description) in [
        (
            &lane_directory,
            &lane_directory_identity,
            "tombstone lane directory",
        ),
        (
            &shards_directory,
            &shards_directory_identity,
            "tombstone shards directory",
        ),
    ] {
        if !crate::engine::fs_utils::path_matches_plain_directory_identity(directory, identity)? {
            return Err(TsinkError::DataCorruption(format!(
                "{description} identity changed during final-orphan discovery: {}",
                directory.display()
            )));
        }
    }
    let fingerprint_scratch = usize::try_from(manifest_fingerprint.logical_bytes)
        .map_err(|_| {
            TsinkError::Other(
                "tombstone final-orphan manifest length exceeds the supported range".to_string(),
            )
        })?
        .checked_add(TOMBSTONE_CLEANUP_ALLOCATOR_SLACK)
        .ok_or_else(|| {
            TsinkError::Other("tombstone cleanup fingerprint memory overflow".to_string())
        })?;
    admit_memory(
        base_retained_bytes
            .checked_add(discovery_path_retained)
            .and_then(|bytes| bytes.checked_add(reference_set_retained))
            .and_then(|bytes| {
                bytes.checked_add(tombstone_cleanup_orphan_paths_retained_bytes(
                    orphan_paths.capacity(),
                    orphan_path_payload_bytes,
                ))
            })
            .and_then(|bytes| bytes.checked_add(fingerprint_scratch))
            .ok_or_else(|| {
                TsinkError::Other("tombstone cleanup fingerprint memory overflow".to_string())
            })?,
    )?;
    let expected_len = manifest_fingerprint
        .exists
        .then(|| usize::try_from(manifest_fingerprint.logical_bytes))
        .transpose()
        .map_err(|_| {
            TsinkError::Other(
                "tombstone final-orphan manifest length exceeds the supported range".to_string(),
            )
        })?;
    if revalidate_remote_tombstone_manifest(&lane, expected_len)? != manifest_fingerprint {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone manifest changed during final-orphan discovery: {}",
            lane.manifest_path.display()
        )));
    }

    if orphan_paths.is_empty() {
        return Ok(None);
    }
    drop(referenced);
    let plan = TombstoneFinalOrphanCleanupPlan {
        lane,
        lane_directory,
        lane_directory_identity,
        shards_directory,
        shards_directory_identity,
        manifest_fingerprint,
        orphan_paths,
    };
    let retained = base_retained_bytes
        .checked_add(std::mem::size_of::<TombstoneFinalOrphanCleanupPlan>())
        .and_then(|bytes| plan.modeled_heap_bytes().ok()?.checked_add(bytes))
        .ok_or_else(|| {
            TsinkError::Other("tombstone cleanup retained-memory overflow".to_string())
        })?;
    admit_memory(
        retained
            .checked_add(plan.execution_scratch_bytes()?)
            .ok_or_else(|| {
                TsinkError::Other("tombstone cleanup execution memory overflow".to_string())
            })?,
    )?;
    Ok(Some(plan))
}

#[allow(dead_code)] // Used by the retained standalone cleanup adapter.
fn combine_tombstone_final_orphan_cleanup_results(
    cleanup_result: Result<u64>,
    settlement_result: Result<()>,
    reconciliation_result: Result<()>,
    removed: u64,
) -> Result<u64> {
    let mut errors = Vec::new();
    if let Err(err) = &cleanup_result {
        errors.push(format!("cleanup failed: {err}"));
    }
    if let Err(err) = &settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = &reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    match errors.len() {
        0 => Ok(removed),
        1 => match (cleanup_result, settlement_result, reconciliation_result) {
            (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
            _ => unreachable!("one recorded tombstone cleanup error must have a failed result"),
        },
        _ => Err(TsinkError::Other(format!(
            "batched budgeted exact-file tombstone cleanup failed: {}",
            errors.join("; ")
        ))),
    }
}

#[cfg(test)]
pub(crate) fn tombstone_store_sidecar_path(path: &Path) -> PathBuf {
    tombstone_store_dir(path)
}

#[cfg(test)]
pub(crate) fn referenced_tombstone_shard_files(path: &Path) -> Result<Vec<Option<String>>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            })
        }
    };

    Ok(load_store_manifest_from_bytes(&bytes)?
        .map(|manifest| manifest.shards)
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;

    fn cleanup_lane(path: &Path) -> TombstoneLane {
        TombstoneLane {
            role: TombstoneLaneRole::LocalNumeric,
            namespace_root: path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            manifest_path: path.to_path_buf(),
        }
    }

    fn regular_file_count_beneath(path: &Path) -> usize {
        let Ok(entries) = std::fs::read_dir(path) else {
            return 0;
        };
        entries
            .map(|entry| entry.unwrap().path())
            .map(|path| {
                if path.is_dir() {
                    regular_file_count_beneath(&path)
                } else {
                    usize::from(path.is_file())
                }
            })
            .sum()
    }

    #[test]
    fn fixed_layout_parsers_reject_oversized_counts_duplicates_and_trailing_bytes() {
        let mut manifest = encode_store_manifest(&empty_store_manifest()).unwrap();
        manifest[12..20].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            decode_store_manifest(&manifest),
            Err(TsinkError::DataCorruption(_))
        ));

        let mut trailing_manifest = encode_store_manifest(&empty_store_manifest()).unwrap();
        trailing_manifest.push(0);
        assert!(matches!(
            decode_store_manifest(&trailing_manifest),
            Err(TsinkError::DataCorruption(_))
        ));

        let duplicate = encode_shard(vec![
            TombstoneSeriesEntryV1 {
                series_id: 7,
                ranges: vec![TombstoneRange { start: 1, end: 2 }],
            },
            TombstoneSeriesEntryV1 {
                series_id: 7,
                ranges: vec![TombstoneRange { start: 3, end: 4 }],
            },
        ])
        .unwrap();
        assert!(matches!(
            preflight_shard_bytes(&duplicate, Some(tombstone_shard_index(7))),
            Err(TsinkError::DataCorruption(_))
        ));

        let wrong_shard = encode_shard(vec![TombstoneSeriesEntryV1 {
            series_id: 7,
            ranges: vec![TombstoneRange { start: 1, end: 2 }],
        }])
        .unwrap();
        assert!(matches!(
            preflight_shard_bytes(&wrong_shard, Some(tombstone_shard_index(7) + 1)),
            Err(TsinkError::DataCorruption(_))
        ));
    }

    #[test]
    fn coordinator_preflight_rejects_lane_and_path_lengths_before_bincode() {
        let record = TombstoneTransactionRecord {
            version: TOMBSTONE_TRANSACTION_VERSION,
            transaction_id: 1,
            phase: TombstoneTransactionPhase::Prepared,
            lanes: vec![TombstoneTransactionLaneRecord {
                role: TombstoneLaneRole::LocalNumeric,
                namespace_root_identity: PathBuf::from("data"),
                manifest_path_identity: PathBuf::from("data/lane_numeric/tombstones.json"),
                previous_manifest: None,
                candidate_manifest: None,
                candidate_shards: Vec::new(),
            }],
        };
        let payload = tombstone_transaction_bincode_options()
            .serialize(&record)
            .unwrap();

        let mut oversized_lane_count = payload.clone();
        oversized_lane_count[14..22].copy_from_slice(&9u64.to_le_bytes());
        assert!(matches!(
            preflight_tombstone_transaction_payload(&oversized_lane_count),
            Err(TsinkError::DataCorruption(_))
        ));

        let mut oversized_path = payload;
        oversized_path[26..34].copy_from_slice(
            &u64::try_from(MAX_TOMBSTONE_TRANSACTION_PATH_BYTES + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert!(matches!(
            preflight_tombstone_transaction_payload(&oversized_path),
            Err(TsinkError::DataCorruption(_))
        ));
    }

    #[test]
    fn coordinator_encoding_admits_payload_and_frame_concurrently() {
        let record = TombstoneTransactionRecord {
            version: TOMBSTONE_TRANSACTION_VERSION,
            transaction_id: 1,
            phase: TombstoneTransactionPhase::Prepared,
            lanes: vec![TombstoneTransactionLaneRecord {
                role: TombstoneLaneRole::LocalNumeric,
                namespace_root_identity: PathBuf::from("data"),
                manifest_path_identity: PathBuf::from("data/lane_numeric/tombstones.json"),
                previous_manifest: Some(vec![0x5a; MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES / 2]),
                candidate_manifest: None,
                candidate_shards: Vec::new(),
            }],
        };
        let retained = transaction_record_retained_bytes(&record);
        let encoded_len = tombstone_transaction_encoded_len(&record).unwrap();
        assert!(encoded_len > 8 * 1024 * 1024);

        let one_buffer_budget = retained
            .saturating_add(encoded_len)
            .saturating_add(TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK);
        let mut rejected_peak = 0usize;
        let err = encode_tombstone_transaction_record_with_memory_admission(
            &record,
            retained,
            |required| {
                rejected_peak = rejected_peak.max(required);
                if required > one_buffer_budget {
                    Err(TsinkError::MemoryBudgetExceeded {
                        budget: one_buffer_budget,
                        required,
                    })
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("a one-buffer budget must reject before coordinator encoding");
        assert!(matches!(err, TsinkError::MemoryBudgetExceeded { .. }));
        assert_eq!(
            rejected_peak,
            retained
                .saturating_add(encoded_len.saturating_mul(2))
                .saturating_add(TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK)
        );

        let two_buffer_budget = rejected_peak;
        let mut admitted_peak = 0usize;
        let encoded = encode_tombstone_transaction_record_with_memory_admission(
            &record,
            retained,
            |required| {
                admitted_peak = admitted_peak.max(required);
                if required > two_buffer_budget {
                    Err(TsinkError::MemoryBudgetExceeded {
                        budget: two_buffer_budget,
                        required,
                    })
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(encoded.len(), encoded_len);
        assert!(admitted_peak <= two_buffer_budget);
    }

    #[test]
    fn transaction_path_and_owned_shard_staging_is_admitted_before_allocation() {
        let temp_dir = TempDir::new().unwrap();
        let lane_root = temp_dir.path().join("r".repeat(120));
        std::fs::create_dir_all(&lane_root).unwrap();
        let manifest_path = lane_root.join(TOMBSTONES_FILE_NAME);
        let lane = TombstoneLane {
            role: TombstoneLaneRole::LocalNumeric,
            namespace_root: lane_root,
            manifest_path: manifest_path.clone(),
        };
        let mut manifest = empty_store_manifest();
        let mut new_shards = Vec::with_capacity(TOMBSTONE_STORE_SHARD_COUNT);
        let mut candidate_shards = Vec::with_capacity(TOMBSTONE_STORE_SHARD_COUNT);
        for shard_index in 0..TOMBSTONE_STORE_SHARD_COUNT {
            let file_name = format!(
                "shard-{shard_index:03}-{:016x}.bin",
                shard_index.saturating_add(1)
            );
            manifest.shards[shard_index] = Some(file_name.clone());
            new_shards.push(PreparedTombstoneShard {
                path: tombstone_shards_dir(&manifest_path).join(&file_name),
                file_name: file_name.clone(),
                payload: vec![u8::try_from(shard_index).unwrap_or(u8::MAX)],
            });
            candidate_shards.push(TombstoneTransactionShardRecord {
                shard_index: u16::try_from(shard_index).unwrap(),
                file_name,
                logical_bytes: 1,
                xxh64: shard_index as u64,
            });
        }
        let plans = vec![PreparedTombstoneStoreUpdate {
            lane: lane.clone(),
            path: manifest_path,
            previous_bytes: None,
            next_manifest_payload: Some(encode_store_manifest(&manifest).unwrap()),
            new_shards,
            candidate_shards,
        }];
        let plans_retained = plans.iter().fold(0usize, |total, plan| {
            total.saturating_add(prepared_tombstone_plan_retained_bytes(plan))
        });
        let probe_record =
            transaction_record_from_plans(&plans, TombstoneTransactionPhase::Prepared);
        let record_retained = transaction_record_retained_bytes(&probe_record);
        let encoded_capacity = encode_tombstone_transaction_record(&probe_record)
            .unwrap()
            .capacity();
        let required = plans_retained
            .saturating_add(record_retained.saturating_mul(2))
            .saturating_add(encoded_capacity.saturating_mul(2))
            .saturating_add(tombstone_transaction_path_staging_upper_bound(
                temp_dir.path(),
                &plans,
            ))
            .saturating_add(tombstone_owned_shard_paths_retained_upper_bound(&plans))
            .saturating_add(TOMBSTONE_TRANSACTION_ENCODE_ALLOCATOR_SLACK);
        let budget = required.saturating_sub(1);
        let mut peak = 0usize;

        let err = persist_tombstone_plans_transactionally(
            temp_dir.path(),
            std::slice::from_ref(&lane),
            plans,
            None,
            crate::DiskReservationKind::Maintenance,
            |requested| {
                peak = peak.max(requested);
                if requested > budget {
                    Err(TsinkError::MemoryBudgetExceeded {
                        budget,
                        required: requested,
                    })
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("target-list and owned-path staging must be admitted before filesystem work");
        assert!(err.is_definitively_clean());
        assert!(matches!(
            err.into_tsink_error(),
            TsinkError::MemoryBudgetExceeded { .. }
        ));
        assert_eq!(peak, required);
        assert!(!tombstone_transaction_dir(temp_dir.path()).exists());
        assert!(!tombstone_store_dir(&lane.manifest_path).exists());
    }

    #[test]
    fn recovery_rejects_mismatched_lane_count_without_panicking() {
        let temp_dir = TempDir::new().unwrap();
        let configured = normalize_tombstone_lanes(&[TombstoneLane {
            role: TombstoneLaneRole::LocalNumeric,
            namespace_root: temp_dir.path().to_path_buf(),
            manifest_path: temp_dir
                .path()
                .join("lane_numeric")
                .join(TOMBSTONES_FILE_NAME),
        }])
        .unwrap();
        let first = &configured[0];
        let record = TombstoneTransactionRecord {
            version: TOMBSTONE_TRANSACTION_VERSION,
            transaction_id: 1,
            phase: TombstoneTransactionPhase::Prepared,
            lanes: vec![
                TombstoneTransactionLaneRecord {
                    role: first.role,
                    namespace_root_identity: first.namespace_root.clone(),
                    manifest_path_identity: first.manifest_path.clone(),
                    previous_manifest: None,
                    candidate_manifest: None,
                    candidate_shards: Vec::new(),
                },
                TombstoneTransactionLaneRecord {
                    role: TombstoneLaneRole::LocalBlob,
                    namespace_root_identity: first.namespace_root.clone(),
                    manifest_path_identity: temp_dir
                        .path()
                        .join("lane_blob")
                        .join(TOMBSTONES_FILE_NAME),
                    previous_manifest: None,
                    candidate_manifest: None,
                    candidate_shards: Vec::new(),
                },
            ],
        };
        let coordinator = tombstone_transaction_path(temp_dir.path());
        std::fs::create_dir_all(coordinator.parent().unwrap()).unwrap();
        std::fs::write(
            &coordinator,
            encode_tombstone_transaction_record(&record).unwrap(),
        )
        .unwrap();

        let err = recover_tombstone_transaction(temp_dir.path(), &configured, None)
            .expect_err("a mismatched coordinator lane count must be rejected");
        assert!(matches!(err, TsinkError::DataCorruption(_)));
        assert!(coordinator.is_file());
    }

    #[test]
    fn recovery_tiny_memory_rejection_precedes_atomic_temp_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let lane_root = temp_dir.path().join("lane_numeric");
        std::fs::create_dir_all(&lane_root).unwrap();
        let lanes = [TombstoneLane {
            role: TombstoneLaneRole::LocalNumeric,
            namespace_root: lane_root.clone(),
            manifest_path: lane_root.join(TOMBSTONES_FILE_NAME),
        }];
        let coordinator_dir = tombstone_transaction_dir(temp_dir.path());
        std::fs::create_dir_all(&coordinator_dir).unwrap();
        let temporary = coordinator_dir.join(format!(
            ".{TOMBSTONE_TRANSACTION_FILE_NAME}.tmp-{}-0000000000000001",
            std::process::id()
        ));
        std::fs::write(&temporary, b"preserve-pending-temp").unwrap();
        let mut peak = 0usize;

        let err = recover_tombstone_transaction_with_memory_admission(
            temp_dir.path(),
            &lanes,
            None,
            |required| {
                peak = peak.max(required);
                if required > 1 {
                    Err(TsinkError::MemoryBudgetExceeded {
                        budget: 1,
                        required,
                    })
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("tiny startup memory must reject before atomic-temp cleanup");
        assert!(matches!(err, TsinkError::MemoryBudgetExceeded { .. }));
        assert!(peak >= tombstone_cleanup_entry_transient_bytes(&coordinator_dir));
        assert_eq!(std::fs::read(temporary).unwrap(), b"preserve-pending-temp");
    }

    #[test]
    fn recovery_atomic_temp_cleanup_batches_managed_removals_into_one_reconciliation() {
        let temp_dir = TempDir::new().unwrap();
        let lane_root = temp_dir.path().join("lane_numeric");
        let manifest_path = lane_root.join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&manifest_path);
        let coordinator_dir = tombstone_transaction_dir(temp_dir.path());
        std::fs::create_dir_all(&shards_dir).unwrap();
        std::fs::create_dir_all(&coordinator_dir).unwrap();
        let pid = std::process::id();
        let temporaries = [
            coordinator_dir.join(format!(
                ".{TOMBSTONE_TRANSACTION_FILE_NAME}.tmp-{pid}-0000000000000001"
            )),
            lane_root.join(format!(
                ".{TOMBSTONES_FILE_NAME}.tmp-{pid}-0000000000000002"
            )),
            shards_dir.join(format!(
                ".shard-007-0000000000000003.bin.tmp-{pid}-0000000000000004"
            )),
        ];
        for temporary in &temporaries {
            std::fs::write(temporary, b"stale").unwrap();
        }
        let unknown = lane_root.join(format!(".{TOMBSTONES_FILE_NAME}.tmp-{pid}-NOT-LOWER-HEX"));
        std::fs::write(&unknown, b"preserve-unknown").unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        let lanes = [TombstoneLane {
            role: TombstoneLaneRole::LocalNumeric,
            namespace_root: lane_root,
            manifest_path,
        }];

        assert_eq!(
            recover_tombstone_transaction(temp_dir.path(), &lanes, Some(&budget)).unwrap(),
            TombstoneRecoveryOutcome::NoTransaction
        );

        assert!(temporaries.iter().all(|path| !path.exists()));
        assert_eq!(std::fs::read(unknown).unwrap(), b"preserve-unknown");
        let after = budget.snapshot();
        assert_eq!(
            after.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            after.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp_dir.path()).unwrap()
        );
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn tombstone_cleanup_definite_noop_skips_reconciliation() {
        let temp_dir = TempDir::new().unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        let missing = [
            temp_dir.path().join("lane_numeric/missing-first.bin"),
            temp_dir.path().join("lane_numeric/missing-second.bin"),
        ];

        let removed = remove_owned_regular_files_and_sync_parents_budgeted(
            missing.iter().map(PathBuf::as_path),
            Some(&budget),
            crate::DiskCategory::Tombstones,
            |_| Ok(()),
        )
        .unwrap();

        assert_eq!(removed, 0);
        let after = budget.snapshot();
        assert_eq!(after.reconciliations_total, before.reconciliations_total);
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn persist_tombstones_returns_error_when_parent_sync_fails() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(TOMBSTONES_FILE_NAME);
        let mut tombstones = TombstoneMap::new();
        tombstones.insert(7, vec![TombstoneRange { start: 10, end: 20 }]);

        let _guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
            {
                let parent = std::fs::canonicalize(path.parent().unwrap()).unwrap();
                let path = path.clone();
                move |candidate| candidate == parent && path.exists()
            },
            "injected parent directory sync failure",
        );
        let err = persist_tombstones(&path, &tombstones)
            .expect_err("parent directory sync failure must be surfaced");
        assert!(
            err.to_string()
                .contains("injected parent directory sync failure"),
            "unexpected error: {err:?}"
        );

        assert!(
            path.exists(),
            "persisted tombstones should remain for retry"
        );
        assert_eq!(load_tombstones(&path).unwrap(), tombstones);
    }

    #[test]
    fn persist_tombstone_updates_rewrites_only_changed_shards() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(TOMBSTONES_FILE_NAME);
        let mut first = TombstoneMap::new();
        first.insert(1, vec![TombstoneRange { start: 10, end: 20 }]);
        persist_tombstone_updates(&path, &first).unwrap();

        let shard_files_after_first = referenced_tombstone_shard_files(&path).unwrap();
        assert_eq!(
            shard_files_after_first.iter().flatten().count(),
            1,
            "first update should only materialize one shard"
        );

        let mut second = TombstoneMap::new();
        second.insert(2, vec![TombstoneRange { start: 30, end: 40 }]);
        persist_tombstone_updates(&path, &second).unwrap();

        let shard_files_after_second = referenced_tombstone_shard_files(&path).unwrap();
        assert_eq!(
            shard_files_after_second.iter().flatten().count(),
            2,
            "second update should only add the newly touched shard"
        );
        assert_eq!(
            shard_files_after_second[tombstone_shard_index(1)],
            shard_files_after_first[tombstone_shard_index(1)],
            "untouched shard should not be rewritten"
        );
        assert_ne!(
            shard_files_after_second[tombstone_shard_index(2)],
            shard_files_after_first[tombstone_shard_index(2)],
            "changed shard should get a new file"
        );
        assert_eq!(
            load_tombstones(&path).unwrap(),
            TombstoneMap::from([
                (1, vec![TombstoneRange { start: 10, end: 20 }]),
                (2, vec![TombstoneRange { start: 30, end: 40 }]),
            ])
        );
    }

    #[test]
    fn tombstone_transaction_preflights_the_complete_multi_lane_quota() {
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("existing.bin"), vec![0u8; 128]).unwrap();
        let budget = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(129),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        let paths = [
            temp_dir
                .path()
                .join("lane_numeric")
                .join(TOMBSTONES_FILE_NAME),
            temp_dir.path().join("lane_blob").join(TOMBSTONES_FILE_NAME),
        ];
        let updates = TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]);
        let before = budget.snapshot();

        let err = persist_tombstone_updates_across_paths_with_disk_budget(
            &paths,
            &updates,
            Some(&budget),
        )
        .expect_err("the complete multi-lane transaction must be rejected before mutation");
        assert!(
            matches!(
                &err,
                TsinkError::DiskQuotaExceeded { .. }
                    | TsinkError::InsufficientCompactionHeadroom { .. }
            ),
            "unexpected error: {err:?}"
        );

        for path in &paths {
            assert!(!path.exists());
            assert!(!tombstone_store_dir(path).exists());
            assert!(load_tombstones(path).unwrap().is_empty());
        }
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);

        drop(budget);
        let reopened = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(129),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.snapshot().accounted_bytes, before.accounted_bytes);
        for path in &paths {
            assert!(load_tombstones(path).unwrap().is_empty());
        }
    }

    #[test]
    fn committed_interruption_between_lane_manifests_rolls_forward_every_lane() {
        let temp_dir = TempDir::new().unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let numeric_path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let blob_path = temp_dir.path().join("lane_blob").join(TOMBSTONES_FILE_NAME);
        let paths = [numeric_path.clone(), blob_path.clone()];
        let initial = TombstoneMap::from([(1, vec![TombstoneRange { start: 10, end: 20 }])]);
        persist_tombstone_updates_across_paths_with_disk_budget(&paths, &initial, Some(&budget))
            .unwrap();
        let guard = fail_tombstone_transaction_once(
            TombstoneTransactionTestPoint::BeforeManifest(1),
            "injected interruption before the second tombstone manifest",
        );
        let updates = TombstoneMap::from([(2, vec![TombstoneRange { start: 30, end: 40 }])]);
        let result = persist_tombstone_updates_across_paths_with_disk_budget(
            &paths,
            &updates,
            Some(&budget),
        );
        assert!(
            result.is_err(),
            "the injected second-lane manifest failure must be surfaced"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected interruption before the second tombstone manifest"),
            "unexpected error: {err:?}"
        );

        let expected = TombstoneMap::from([
            (1, vec![TombstoneRange { start: 10, end: 20 }]),
            (2, vec![TombstoneRange { start: 30, end: 40 }]),
        ]);
        assert_eq!(load_tombstones(&numeric_path).unwrap(), expected);
        assert_eq!(load_tombstones(&blob_path).unwrap(), initial);
        assert!(tombstone_transaction_path(budget.root()).is_file());

        drop(guard);
        let lanes = [
            TombstoneLane {
                role: TombstoneLaneRole::LocalNumeric,
                namespace_root: numeric_path.parent().unwrap().to_path_buf(),
                manifest_path: numeric_path.clone(),
            },
            TombstoneLane {
                role: TombstoneLaneRole::LocalBlob,
                namespace_root: blob_path.parent().unwrap().to_path_buf(),
                manifest_path: blob_path.clone(),
            },
        ];
        assert_eq!(
            recover_tombstone_transaction(budget.root(), &lanes, Some(&budget)).unwrap(),
            TombstoneRecoveryOutcome::RolledForwardCommitted
        );
        assert_eq!(load_tombstones(&numeric_path).unwrap(), expected);
        assert_eq!(load_tombstones(&blob_path).unwrap(), expected);
        assert_eq!(
            recover_tombstone_transaction(budget.root(), &lanes, Some(&budget)).unwrap(),
            TombstoneRecoveryOutcome::NoTransaction
        );
        let after = budget.snapshot();
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
        assert_eq!(
            regular_file_count_beneath(&tombstone_store_dir(&numeric_path)),
            2
        );
        assert_eq!(
            regular_file_count_beneath(&tombstone_store_dir(&blob_path)),
            2
        );
    }

    #[test]
    fn partial_multi_shard_failure_removes_every_staged_shard() {
        let temp_dir = TempDir::new().unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let paths = [path.clone()];
        let initial = TombstoneMap::from([(1, vec![TombstoneRange { start: 10, end: 20 }])]);
        persist_tombstone_updates_across_paths_with_disk_budget(&paths, &initial, Some(&budget))
            .unwrap();
        let manifest_before = std::fs::read(&path).unwrap();
        let shards_before = referenced_tombstone_shard_files(&path).unwrap();
        let before = budget.snapshot();

        let shard_syncs = Arc::new(AtomicUsize::new(0));
        let shards_dir = std::fs::canonicalize(tombstone_shards_dir(&path)).unwrap();
        let _guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
            {
                let shard_syncs = Arc::clone(&shard_syncs);
                move |candidate| {
                    candidate == shards_dir && shard_syncs.fetch_add(1, Ordering::SeqCst) == 1
                }
            },
            "injected second tombstone shard failure",
        );
        let updates = TombstoneMap::from([
            (2, vec![TombstoneRange { start: 30, end: 40 }]),
            (3, vec![TombstoneRange { start: 50, end: 60 }]),
        ]);
        let err = persist_tombstone_updates_across_paths_with_disk_budget(
            &paths,
            &updates,
            Some(&budget),
        )
        .expect_err("the injected second-shard failure must be surfaced");
        assert!(
            err.to_string()
                .contains("injected second tombstone shard failure"),
            "unexpected error: {err:?}"
        );

        assert_eq!(std::fs::read(&path).unwrap(), manifest_before);
        assert_eq!(load_tombstones(&path).unwrap(), initial);
        assert_eq!(
            referenced_tombstone_shard_files(&path).unwrap(),
            shards_before
        );
        assert_eq!(regular_file_count_beneath(&tombstone_store_dir(&path)), 1);
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn unmanaged_tombstone_ancestry_sync_failure_is_surfaced_without_local_quota_charge() {
        let temp_dir = TempDir::new().unwrap();
        let local_root = temp_dir.path().join("local");
        let external_root = temp_dir.path().join("object-store");
        let budget = crate::LocalDiskBudget::open(
            &local_root,
            crate::LocalDiskLimits {
                max_bytes: Some(64 * 1024),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        let path = external_root
            .join("hot")
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let updates = TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]);
        let before = budget.snapshot();

        let guard = crate::engine::fs_utils::fail_directory_sync_once(
            resolve_trusted_namespace_root(&external_root).unwrap(),
            "injected external tombstone ancestry sync failure",
        );
        let err = persist_tombstone_updates_across_paths_with_disk_budget(
            std::slice::from_ref(&path),
            &updates,
            Some(&budget),
        )
        .expect_err("external tombstone ancestry must be synchronized before shard publication");
        assert!(
            err.to_string()
                .contains("injected external tombstone ancestry sync failure"),
            "unexpected error: {err:?}"
        );
        assert!(!path.exists());
        assert_eq!(regular_file_count_beneath(&tombstone_store_dir(&path)), 0);
        assert_eq!(budget.snapshot().accounted_bytes, before.accounted_bytes);

        drop(guard);
        persist_tombstone_updates_across_paths_with_disk_budget(
            std::slice::from_ref(&path),
            &updates,
            Some(&budget),
        )
        .unwrap();
        assert_eq!(load_tombstones(&path).unwrap(), updates);
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn corrupt_v2_manifest_magic_preserves_all_candidate_shards() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let expected = TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]);
        persist_tombstone_updates(&path, &expected).unwrap();

        let manifest_before = std::fs::read(&path).unwrap();
        let shard_file_name = referenced_tombstone_shard_files(&path)
            .unwrap()
            .into_iter()
            .flatten()
            .next()
            .unwrap();
        let shard_path = tombstone_shards_dir(&path).join(shard_file_name);
        let shard_before = std::fs::read(&shard_path).unwrap();
        let mut corrupt_manifest = manifest_before.clone();
        corrupt_manifest[0] ^= 0x01;
        std::fs::write(&path, corrupt_manifest).unwrap();

        cleanup_unreferenced_tombstone_shards(&cleanup_lane(&path), None)
            .expect_err("unrecognized manifest bytes must fail closed before orphan cleanup");
        assert_eq!(std::fs::read(&shard_path).unwrap(), shard_before);
        load_tombstones(&path).expect_err("the corrupt manifest must remain visible to recovery");

        std::fs::write(&path, manifest_before).unwrap();
        assert_eq!(load_tombstones(&path).unwrap(), expected);
    }

    #[test]
    fn orphan_cleanup_bounds_external_manifest_before_scanning_candidates() {
        let temp_dir = TempDir::new().unwrap();
        let data_root = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_root).unwrap();
        let budget =
            crate::LocalDiskBudget::open(&data_root, crate::LocalDiskLimits::default()).unwrap();
        let object_root = temp_dir.path().join("object-store");
        let path = object_root
            .join("hot")
            .join("numeric")
            .join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&path);
        std::fs::create_dir_all(&shards_dir).unwrap();
        let orphan = shards_dir.join("shard-007-0000000000000001.bin");
        std::fs::write(&orphan, b"preserve-me").unwrap();
        let manifest = std::fs::File::create(&path).unwrap();
        manifest
            .set_len((MAX_TOMBSTONE_TRANSACTION_RECORD_BYTES as u64) + 1)
            .unwrap();
        drop(manifest);
        let lane = TombstoneLane {
            role: TombstoneLaneRole::HotNumeric,
            namespace_root: object_root,
            manifest_path: path,
        };

        let err = cleanup_unreferenced_tombstone_shards(&lane, Some(&budget))
            .expect_err("an oversized external manifest must fail before orphan scanning");
        assert!(matches!(err, TsinkError::DataCorruption(_)));
        assert_eq!(std::fs::read(orphan).unwrap(), b"preserve-me");
    }

    #[test]
    fn orphan_cleanup_tiny_memory_rejection_preserves_every_candidate() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&path);
        std::fs::create_dir_all(&shards_dir).unwrap();
        let first = shards_dir.join("shard-007-0000000000000001.bin");
        let second = shards_dir.join("shard-008-0000000000000002.bin");
        std::fs::write(&first, b"preserve-first").unwrap();
        std::fs::write(&second, b"preserve-second").unwrap();
        let mut requested = 0usize;

        let err = cleanup_unreferenced_tombstone_shards_with_memory_admission(
            &cleanup_lane(&path),
            None,
            |required| {
                requested = requested.max(required);
                if required > 1 {
                    Err(TsinkError::MemoryBudgetExceeded {
                        budget: 1,
                        required,
                    })
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("tiny startup memory must reject before orphan enumeration or removal");
        assert!(matches!(err, TsinkError::MemoryBudgetExceeded { .. }));
        assert!(requested >= tombstone_cleanup_entry_transient_bytes(&shards_dir));
        assert_eq!(std::fs::read(first).unwrap(), b"preserve-first");
        assert_eq!(std::fs::read(second).unwrap(), b"preserve-second");
    }

    #[test]
    fn orphan_cleanup_batches_managed_removals_into_one_reconciliation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&path);
        std::fs::create_dir_all(&shards_dir).unwrap();
        let orphans = [
            shards_dir.join("shard-007-0000000000000001.bin"),
            shards_dir.join("shard-008-0000000000000002.bin"),
            shards_dir.join("shard-009-0000000000000003.bin"),
        ];
        for orphan in &orphans {
            std::fs::write(orphan, b"orphan").unwrap();
        }
        let unknown = shards_dir.join("shard-999-0000000000000004.bin");
        std::fs::write(&unknown, b"preserve-unknown").unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();

        assert_eq!(
            cleanup_unreferenced_tombstone_shards(&cleanup_lane(&path), Some(&budget)).unwrap(),
            3
        );

        assert!(orphans.iter().all(|orphan| !orphan.exists()));
        assert_eq!(std::fs::read(unknown).unwrap(), b"preserve-unknown");
        let after = budget.snapshot();
        assert_eq!(
            after.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            after.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp_dir.path()).unwrap()
        );
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn orphan_cleanup_reconciles_once_after_post_unlink_sync_failure() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&path);
        std::fs::create_dir_all(&shards_dir).unwrap();
        let orphans = [
            shards_dir.join("shard-007-0000000000000001.bin"),
            shards_dir.join("shard-008-0000000000000002.bin"),
        ];
        for orphan in &orphans {
            std::fs::write(orphan, b"orphan").unwrap();
        }
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            std::fs::canonicalize(&shards_dir).unwrap(),
            "injected tombstone orphan cleanup sync failure",
        );

        let err = cleanup_unreferenced_tombstone_shards(&cleanup_lane(&path), Some(&budget))
            .expect_err("a committed orphan unlink must retain its synchronization error");

        assert!(matches!(
            err,
            TsinkError::Other(ref message)
                if message == "injected tombstone orphan cleanup sync failure"
        ));
        assert_eq!(orphans.iter().filter(|orphan| orphan.exists()).count(), 1);
        let after = budget.snapshot();
        assert_eq!(
            after.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            after.accounted_bytes,
            crate::disk_budget::measured_path_bytes(temp_dir.path()).unwrap()
        );
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn orphan_cleanup_rejects_owned_shard_directory_without_recursive_removal() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let candidate = tombstone_shards_dir(&path).join("shard-007-0000000000000001.bin");
        std::fs::create_dir_all(&candidate).unwrap();
        std::fs::write(candidate.join("sentinel"), b"preserve-me").unwrap();

        let err = cleanup_unreferenced_tombstone_shards(&cleanup_lane(&path), None)
            .expect_err("an owned-name directory must never reach recursive cleanup");
        assert!(matches!(err, TsinkError::DataCorruption(_)));
        assert_eq!(
            std::fs::read(candidate.join("sentinel")).unwrap(),
            b"preserve-me"
        );
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn exact_reader_does_not_follow_file_swapped_to_link_after_precheck() {
        let temp_dir = TempDir::new().unwrap();
        let owned = temp_dir.path().join("owned.bin");
        let external = temp_dir.path().join("external.bin");
        std::fs::write(&owned, b"owned-payload").unwrap();
        std::fs::write(&external, b"secret-outside").unwrap();

        #[cfg(unix)]
        let make_link = |target: &Path, link: &Path| std::os::unix::fs::symlink(target, link);
        #[cfg(windows)]
        let make_link =
            |target: &Path, link: &Path| std::os::windows::fs::symlink_file(target, link);

        // Windows may deny symlink creation when neither Developer Mode nor the privilege is
        // enabled. In that environment the cfg(windows) production helper still compiles, while
        // this runtime race case is skipped.
        let probe = temp_dir.path().join("link-probe.bin");
        if let Err(err) = make_link(&external, &probe) {
            #[cfg(windows)]
            if err.kind() == std::io::ErrorKind::PermissionDenied {
                return;
            }
            panic!("failed to create link probe: {err}");
        }
        std::fs::remove_file(&probe).unwrap();

        let err = read_required_regular_file_bounded_exact_with_before_open(
            &owned,
            1024,
            b"owned-payload".len(),
            || {
                std::fs::remove_file(&owned).unwrap();
                make_link(&external, &owned).unwrap();
            },
        )
        .expect_err("an admitted file swapped to a link must never be followed");
        assert!(matches!(
            err,
            TsinkError::IoWithPath { .. } | TsinkError::DataCorruption(_)
        ));
        assert_eq!(std::fs::read(&external).unwrap(), b"secret-outside");
    }

    #[test]
    fn manifest_rejects_traversal_and_absolute_shard_names_before_read_or_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("lane_numeric");
        std::fs::create_dir_all(&lane_path).unwrap();
        let path = lane_path.join(TOMBSTONES_FILE_NAME);
        let outside = temp_dir.path().join("outside-sentinel.bin");
        std::fs::write(&outside, b"outside-sentinel").unwrap();
        let malicious_names = [
            "../../outside-sentinel.bin".to_string(),
            outside.to_string_lossy().into_owned(),
        ];

        for malicious_name in malicious_names {
            let mut manifest = empty_store_manifest();
            manifest.shards[7] = Some(malicious_name);
            std::fs::write(&path, encode_store_manifest(&manifest).unwrap()).unwrap();

            let load_err = load_tombstones(&path)
                .expect_err("an untrusted manifest shard path must not be read");
            assert!(matches!(load_err, TsinkError::DataCorruption(_)));
            let update_err = persist_tombstone_updates(
                &path,
                &TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]),
            )
            .expect_err("an untrusted manifest shard path must not reach obsolete cleanup");
            assert!(matches!(update_err, TsinkError::DataCorruption(_)));
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside-sentinel");
            assert!(!tombstone_store_dir(&path).exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn referenced_and_orphan_tombstone_shard_symlinks_are_rejected_without_following() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&path);
        std::fs::create_dir_all(&shards_dir).unwrap();
        let external = temp_dir.path().join("external-shard.bin");
        let external_payload = encode_shard(vec![TombstoneSeriesEntryV1 {
            series_id: 7,
            ranges: vec![TombstoneRange { start: 10, end: 20 }],
        }])
        .unwrap();
        std::fs::write(&external, &external_payload).unwrap();
        let file_name = "shard-007-0000000000000001.bin";
        let shard_path = shards_dir.join(file_name);
        symlink(&external, &shard_path).unwrap();
        let mut manifest = empty_store_manifest();
        manifest.shards[7] = Some(file_name.to_string());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, encode_store_manifest(&manifest).unwrap()).unwrap();

        let err = load_tombstones(&path)
            .expect_err("a referenced shard symlink must not be followed outside the store");
        assert!(matches!(err, TsinkError::DataCorruption(_)));
        assert_eq!(std::fs::read(&external).unwrap(), external_payload);
        assert!(std::fs::symlink_metadata(&shard_path)
            .unwrap()
            .file_type()
            .is_symlink());

        std::fs::remove_file(&path).unwrap();
        let cleanup_err = cleanup_unreferenced_tombstone_shards(&cleanup_lane(&path), None)
            .expect_err("an orphan shard symlink must be rejected rather than followed or removed");
        assert!(matches!(cleanup_err, TsinkError::DataCorruption(_)));
        assert_eq!(std::fs::read(&external).unwrap(), external_payload);
        assert!(std::fs::symlink_metadata(&shard_path)
            .unwrap()
            .file_type()
            .is_symlink());
    }
}
