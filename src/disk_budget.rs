//! Shared local-disk accounting and reservation primitives.
//!
//! A budget covers one local data-directory tree. It counts regular files and symlinks without
//! following symlinks, keeps unknown files in the total, and serializes reservations so concurrent
//! writers cannot all admit against the same remaining capacity. Reservations are conservative:
//! writers reserve their peak additional footprint and commit only the final persistent delta.

use crate::{Result, TsinkError};
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Limits enforced by a [`LocalDiskBudget`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDiskLimits {
    /// Maximum bytes accounted beneath the local data-directory root.
    ///
    /// `None` leaves the logical data-directory total unlimited while retaining accounting.
    pub max_bytes: Option<u64>,
    /// Filesystem bytes that all tsink work must leave available.
    pub filesystem_free_headroom_bytes: u64,
    /// Bytes inside `max_bytes` and physical free space reserved for maintenance temporary output.
    pub maintenance_temp_reserve_bytes: u64,
}

impl LocalDiskLimits {
    /// Validates relationships between logical and reserved space.
    pub fn validate(self) -> Result<Self> {
        if self.max_bytes == Some(0) {
            return Err(TsinkError::InvalidConfiguration(
                "local disk limit must be greater than zero".to_string(),
            ));
        }
        if let Some(max_bytes) = self.max_bytes {
            if self.maintenance_temp_reserve_bytes >= max_bytes {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "maintenance temporary reserve {} must be smaller than local disk limit {}",
                    self.maintenance_temp_reserve_bytes, max_bytes
                )));
            }
        }
        self.filesystem_free_headroom_bytes
            .checked_add(self.maintenance_temp_reserve_bytes)
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "filesystem free-space headroom {} plus maintenance temporary reserve {} exceeds the supported byte range",
                    self.filesystem_free_headroom_bytes, self.maintenance_temp_reserve_bytes
                ))
            })?;
        Ok(self)
    }
}

/// Stable accounting categories for files beneath a local data directory.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskCategory {
    /// Write-ahead log segments and WAL publication metadata.
    Wal,
    /// Immutable numeric and blob segment files.
    Segments,
    /// Series registry snapshots, deltas, and registry catalogs.
    Registry,
    /// Delete tombstones and their shards.
    Tombstones,
    /// Rollup policies, checkpoints, and materialization state.
    Rollups,
    /// Server metric-metadata state.
    Metadata,
    /// Server exemplar state.
    Exemplars,
    /// Experimental cluster control, audit, dedupe, and handoff state.
    Cluster,
    /// Edge store-and-forward queues and dedupe state.
    EdgeSync,
    /// Other owned server state, including rules, usage, and managed control state.
    ServerState,
    /// Recognized staging, replacement, backup, and temporary files.
    Temporary,
    /// Files not owned or recognized by the current implementation.
    Unknown,
}

/// Whether a reservation is normal growth or maintenance work allowed to use its reserve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskReservationKind {
    /// Foreground or background growth that must leave the maintenance reserve untouched.
    Growth,
    /// Maintenance temporary output that may consume the configured maintenance reserve.
    Maintenance,
    /// Required recovery/cleanup work that may run while existing usage is above the logical cap.
    Recovery,
}

impl DiskReservationKind {
    fn uses_maintenance_capacity(self) -> bool {
        matches!(self, Self::Maintenance | Self::Recovery)
    }
}

/// Bytes attributed to one disk category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCategoryUsage {
    pub category: DiskCategory,
    pub bytes: u64,
}

/// Current reconciled usage and live reservations for a local data directory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDiskBudgetSnapshot {
    /// Active limits.
    pub limits: LocalDiskLimits,
    /// Reconciled and committed file bytes beneath the budget root.
    pub accounted_bytes: u64,
    /// Bytes held by live reservations but not yet committed.
    pub reserved_bytes: u64,
    /// Portion of `reserved_bytes` held by maintenance work.
    pub maintenance_reserved_bytes: u64,
    /// Reconciled bytes whose files are not recognized as tsink-owned state.
    pub unknown_bytes: u64,
    /// Best-effort current filesystem space available to the current user.
    pub filesystem_available_bytes: Option<u64>,
    /// Whether reconciled usage already exceeds the configured logical limit.
    pub over_limit: bool,
    /// Number of currently live reservations.
    pub active_reservations: u64,
    /// Reservations rejected by the logical quota or physical-space floor.
    pub rejections_total: u64,
    /// Successful full-tree reconciliations, including the initial scan.
    pub reconciliations_total: u64,
    /// Reservation commits whose final growth exceeded the reserved peak.
    ///
    /// Writers must size reservations so this remains zero. A non-zero value means the hard-bound
    /// contract was violated and warrants operator attention.
    pub reservation_overruns_total: u64,
    /// Reconciled bytes by category. Categories with zero bytes are omitted.
    pub categories: Vec<DiskCategoryUsage>,
}

/// Allocation-free local-disk state consumed by the Prometheus metrics adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalDiskMetricsSnapshot {
    pub limits: LocalDiskLimits,
    pub accounted_bytes: u64,
    pub reserved_bytes: u64,
    pub maintenance_reserved_bytes: u64,
    pub unknown_bytes: u64,
    pub filesystem_available_bytes: Option<u64>,
    pub over_limit: bool,
    pub active_reservations: u64,
    pub rejections_total: u64,
    pub reconciliations_total: u64,
    pub reservation_overruns_total: u64,
    category_bytes: [u64; DISK_CATEGORY_COUNT],
}

impl LocalDiskMetricsSnapshot {
    /// Iterates non-empty categories in the same stable order as [`LocalDiskBudgetSnapshot`].
    ///
    /// The iterator borrows the fixed inline category array and never reconstructs a `Vec`.
    pub fn categories(&self) -> impl Iterator<Item = DiskCategoryUsage> + '_ {
        DISK_CATEGORIES
            .iter()
            .copied()
            .zip(self.category_bytes)
            .filter_map(|(category, bytes)| {
                (bytes > 0).then_some(DiskCategoryUsage { category, bytes })
            })
    }
}

/// One complete replacement to stage beneath a shared local-disk budget.
///
/// Grouped replacements reserve the sum of every replacement's encoded length, plus one
/// conservative allocation-unit allowance for each staged temporary entry and each missing
/// parent-directory entry, before mutating the filesystem. The caller can then publish the
/// fully-synchronized temporary files in a chosen order through
/// [`StagedManagedFileReplacements::publish`].
#[derive(Debug, Clone, Copy)]
pub struct ManagedFileReplacement<'a> {
    target: &'a Path,
    bytes: &'a [u8],
}

impl<'a> ManagedFileReplacement<'a> {
    /// Describes one managed target and its complete replacement contents.
    pub fn new(target: &'a Path, bytes: &'a [u8]) -> Self {
        Self { target, bytes }
    }

    /// Returns the managed target that will be replaced.
    pub fn target(self) -> &'a Path {
        self.target
    }

    /// Returns the complete bytes that will be written to the staged temporary file.
    pub fn bytes(self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Debug)]
struct StagedManagedFileReplacement {
    target: PathBuf,
    temporary: PathBuf,
    published: bool,
}

/// Fully-written managed replacement files awaiting ordered atomic publication.
///
/// Values of this type are only borrowed by the closure passed to
/// [`LocalDiskBudget::with_staged_managed_file_replacements`] or its recovery variant. Every
/// temporary file has already been flushed and synchronized before that closure begins. A
/// successful rename is recorded before the target's parent directory is synchronized, so a
/// caller can distinguish a pre-rename failure from an error that happened after the new target
/// became visible.
pub struct StagedManagedFileReplacements {
    budget: Option<Arc<LocalDiskBudget>>,
    category: Option<DiskCategory>,
    reservation: Option<DiskReservation>,
    replacements: Vec<StagedManagedFileReplacement>,
    created_directories: Vec<PathBuf>,
    publication_ambiguous: bool,
    finalized: bool,
}

impl fmt::Debug for StagedManagedFileReplacements {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedManagedFileReplacements")
            .field("category", &self.category)
            .field("replacements", &self.replacements)
            .field("created_directories", &self.created_directories)
            .field("publication_ambiguous", &self.publication_ambiguous)
            .field("finalized", &self.finalized)
            .finish_non_exhaustive()
    }
}

impl StagedManagedFileReplacements {
    /// Returns the number of replacements in this group.
    pub fn len(&self) -> usize {
        self.replacements.len()
    }

    /// Returns whether the group contains no replacements.
    pub fn is_empty(&self) -> bool {
        self.replacements.is_empty()
    }

    /// Returns the managed target at `index`.
    pub fn target(&self, index: usize) -> Option<&Path> {
        self.replacements
            .get(index)
            .map(|replacement| replacement.target.as_path())
    }

    /// Returns the owned temporary path staged for the target at `index`.
    pub fn temporary_path(&self, index: usize) -> Option<&Path> {
        self.replacements
            .get(index)
            .map(|replacement| replacement.temporary.as_path())
    }

    /// Returns whether the replacement at `index` completed its atomic rename.
    ///
    /// This becomes true before parent-directory synchronization. Therefore it remains true when
    /// [`StagedManagedFileReplacements::publish`] returns a parent-sync error after publication.
    pub fn is_published(&self, index: usize) -> bool {
        self.replacements
            .get(index)
            .is_some_and(|replacement| replacement.published)
    }

    /// Returns the number of replacements whose atomic rename completed.
    pub fn published_count(&self) -> usize {
        self.replacements
            .iter()
            .filter(|replacement| replacement.published)
            .count()
    }

    /// Returns whether a rename reported failure and its publication outcome is treated as
    /// ambiguous for conservative accounting.
    pub fn publication_ambiguous(&self) -> bool {
        self.publication_ambiguous
    }

    /// Atomically publishes one staged replacement and synchronizes its target's parent.
    ///
    /// Replacements may be published in any caller-selected order. Publishing an index twice is
    /// rejected. If the rename succeeds but parent synchronization fails, this method returns the
    /// original error while [`StagedManagedFileReplacements::is_published`] remains true.
    pub fn publish(&mut self, index: usize) -> Result<()> {
        let replacement = self.replacements.get(index).ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed replacement index {index} is outside staged group length {}",
                self.replacements.len()
            ))
        })?;
        if replacement.published {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed replacement index {index} was already published: {}",
                replacement.target.display()
            )));
        }

        let temporary = replacement.temporary.clone();
        let target = replacement.target.clone();
        if let Err(err) = crate::engine::fs_utils::rename_tmp(&temporary, &target) {
            // A platform rename API reporting failure normally means the source remains visible,
            // but do not make that assumption part of the hard disk-accounting contract.
            self.publication_ambiguous = true;
            return Err(err);
        }
        self.replacements[index].published = true;
        crate::engine::fs_utils::sync_parent_dir(&target)
    }

    fn cleanup_unpublished(&mut self) -> Result<()> {
        let mut parents_to_sync = BTreeSet::new();
        let mut errors = Vec::new();
        for replacement in &self.replacements {
            if replacement.published {
                continue;
            }
            match crate::engine::fs_utils::remove_file_if_exists(&replacement.temporary) {
                Ok(true) => {
                    if let Some(parent) = replacement.temporary.parent() {
                        parents_to_sync.insert(parent.to_path_buf());
                    }
                }
                Ok(false) => {}
                Err(err) => errors.push(format!(
                    "failed to remove staged temporary {}: {err}",
                    replacement.temporary.display()
                )),
            }
        }
        for parent in parents_to_sync {
            if let Err(err) = crate::engine::fs_utils::sync_dir(&parent) {
                errors.push(format!(
                    "failed to synchronize staged-temporary parent {}: {err}",
                    parent.display()
                ));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(TsinkError::Other(errors.join("; ")))
        }
    }

    fn cleanup_created_directories(&mut self) -> Result<()> {
        if self.published_count() > 0 || self.created_directories.is_empty() {
            return Ok(());
        }

        self.created_directories.sort_by(|left, right| {
            right
                .components()
                .count()
                .cmp(&left.components().count())
                .then_with(|| right.cmp(left))
        });
        let mut errors = Vec::new();
        for directory in &self.created_directories {
            match fs::symlink_metadata(directory) {
                Ok(metadata)
                    if !crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                        && metadata.file_type().is_dir() => {}
                Ok(metadata) => {
                    errors.push(format!(
                        "owned staged parent became {:?} instead of a directory: {}",
                        metadata.file_type(),
                        directory.display()
                    ));
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    errors.push(format!(
                        "failed to inspect owned staged parent {}: {err}",
                        directory.display()
                    ));
                    continue;
                }
            }

            match fs::remove_dir(directory) {
                Ok(()) => {
                    if let Err(err) = crate::engine::fs_utils::sync_parent_dir(directory) {
                        errors.push(format!(
                            "failed to synchronize removed staged parent {}: {err}",
                            directory.display()
                        ));
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => errors.push(format!(
                    "failed to remove owned staged parent {}: {err}",
                    directory.display()
                )),
            }
        }

        if errors.is_empty() {
            self.created_directories.clear();
            Ok(())
        } else {
            Err(TsinkError::Other(errors.join("; ")))
        }
    }

    fn finalize(&mut self) -> Result<()> {
        if self.finalized {
            return Ok(());
        }
        let temporary_cleanup_result = self.cleanup_unpublished();
        let published_count = self.published_count();
        let directory_cleanup_result = self.cleanup_created_directories();
        let cleanup_result = match (temporary_cleanup_result, directory_cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(temporary_error), Err(directory_error)) => Err(TsinkError::Other(format!(
                "temporary cleanup failed: {temporary_error}; staged-parent cleanup failed: {directory_error}"
            ))),
        };
        let all_published = published_count == self.replacements.len();
        let requires_reconciliation = self.budget.is_some()
            && (published_count > 0 || self.publication_ambiguous || cleanup_result.is_err());

        let settlement_result = match self.reservation.take() {
            Some(reservation) if requires_reconciliation && all_published => {
                let reserved_bytes = reservation.reserved_bytes();
                reservation.commit(reserved_bytes, 0)
            }
            Some(reservation) if requires_reconciliation => {
                let reserved_bytes = reservation.reserved_bytes();
                reservation.commit_as(DiskCategory::Temporary, reserved_bytes, 0)
            }
            Some(reservation) => {
                drop(reservation);
                Ok(())
            }
            None => Ok(()),
        };
        let reconciliation_result = if requires_reconciliation {
            self.budget
                .as_ref()
                .expect("budgeted staged replacement must retain its budget")
                .reconcile_when_idle()
                .map(|_| ())
        } else {
            Ok(())
        };

        self.finalized = true;
        combine_grouped_replacement_finalization(
            cleanup_result,
            settlement_result,
            reconciliation_result,
        )
    }
}

impl Drop for StagedManagedFileReplacements {
    fn drop(&mut self) {
        if !self.finalized {
            // Normal returns use the fallible finalizer below. This best-effort path preserves the
            // accounting invariant and removes owned temporaries if the publication closure
            // unwinds or otherwise fails to return normally.
            let _ = self.finalize();
        }
    }
}

fn combine_grouped_replacement_finalization(
    cleanup_result: Result<()>,
    settlement_result: Result<()>,
    reconciliation_result: Result<()>,
) -> Result<()> {
    let mut errors = Vec::new();
    if let Err(err) = cleanup_result {
        errors.push(format!("staged cleanup failed: {err}"));
    }
    if let Err(err) = settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::Other(errors.join("; ")))
    }
}

fn combine_grouped_replacement_operation<T>(
    operation_result: Result<T>,
    finalization_result: Result<()>,
) -> Result<T> {
    match (operation_result, finalization_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(operation_err), Ok(())) => Err(operation_err),
        (Ok(_), Err(finalization_err)) => Err(finalization_err),
        (Err(operation_err), Err(finalization_err)) => Err(TsinkError::Other(format!(
            "grouped managed replacement failed: {operation_err}; finalization failed: {finalization_err}"
        ))),
    }
}

/// Stages complete replacement files before publishing them in a caller-selected order.
///
/// This is the unbudgeted counterpart to
/// [`LocalDiskBudget::with_staged_managed_file_replacements`]. Every owned temporary file is
/// written, flushed, and synchronized before `publish` is invoked. The closure publishes targets
/// with [`StagedManagedFileReplacements::publish`], and unpublished temporaries are removed on
/// every ordinary return. The caller must serialize concurrent operations over the same targets.
///
/// The closure must publish every replacement before returning success.
pub fn with_staged_file_replacements<T, F>(
    replacements: &[ManagedFileReplacement<'_>],
    publish: F,
) -> Result<T>
where
    F: FnOnce(&mut StagedManagedFileReplacements) -> Result<T>,
{
    let mut prepared = Vec::with_capacity(replacements.len());
    let mut distinct_targets = BTreeSet::new();
    for replacement in replacements {
        let target = resolve_entry_path_no_follow(replacement.target)?;
        if !distinct_targets.insert(target.clone()) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "staged file replacement contains duplicate target {}",
                target.display()
            )));
        }
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "staged file replacement target has no parent directory: {}",
                target.display()
            ))
        })?;
        crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;
        validate_regular_file_entry(&target)?;
        prepared.push((target, replacement.bytes));
    }

    let mut staged = StagedManagedFileReplacements {
        budget: None,
        category: None,
        reservation: None,
        replacements: Vec::with_capacity(prepared.len()),
        created_directories: Vec::new(),
        publication_ambiguous: false,
        finalized: false,
    };
    staged.publication_ambiguous = true;
    for (target, bytes) in prepared {
        match crate::engine::fs_utils::write_tmp_and_sync_with_observer(
            &target,
            bytes,
            |temporary| {
                staged.replacements.push(StagedManagedFileReplacement {
                    target: target.clone(),
                    temporary: temporary.to_path_buf(),
                    published: false,
                });
            },
        ) {
            Ok(_) => {}
            Err(stage_err) => {
                let finalization_result = staged.finalize();
                return combine_grouped_replacement_operation(Err(stage_err), finalization_result);
            }
        }
    }
    staged.publication_ambiguous = false;

    let mut operation_result = publish(&mut staged);
    if operation_result.is_ok() && staged.published_count() != staged.len() {
        operation_result = Err(TsinkError::InvalidConfiguration(format!(
            "staged file replacement closure published {} of {} targets",
            staged.published_count(),
            staged.len()
        )));
    }
    let finalization_result = staged.finalize();
    combine_grouped_replacement_operation(operation_result, finalization_result)
}

trait SpaceProbe: Send + Sync {
    fn available_space(&self, path: &Path) -> std::io::Result<u64>;

    fn allocation_unit(&self, _path: &Path) -> std::io::Result<u64> {
        Ok(crate::SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES)
    }
}

#[derive(Debug)]
struct SystemSpaceProbe;

impl SpaceProbe for SystemSpaceProbe {
    fn available_space(&self, path: &Path) -> std::io::Result<u64> {
        system_available_space(path)
    }

    fn allocation_unit(&self, path: &Path) -> std::io::Result<u64> {
        system_allocation_unit(path)
    }
}

#[cfg(test)]
struct FixedAvailableSpaceProbeForTest(u64);

#[cfg(test)]
impl SpaceProbe for FixedAvailableSpaceProbeForTest {
    fn available_space(&self, _path: &Path) -> std::io::Result<u64> {
        Ok(self.0)
    }
}

#[derive(Debug, Default)]
struct DiskAccountingState {
    committed_by_category: BTreeMap<DiskCategory, u64>,
    reserved_bytes: u64,
    maintenance_reserved_bytes: u64,
    active_reservations: u64,
    reconciliation_waiters: u64,
    rejections_total: u64,
    reconciliations_total: u64,
    reservation_overruns_total: u64,
}

impl DiskAccountingState {
    fn accounted_bytes(&self) -> u64 {
        self.committed_by_category
            .values()
            .try_fold(0u64, |total, bytes| total.checked_add(*bytes))
            .expect("disk accounting category total overflowed after validation")
    }

    fn category_bytes(&self, category: DiskCategory) -> u64 {
        self.committed_by_category
            .get(&category)
            .copied()
            .unwrap_or(0)
    }

    fn apply_committed_delta(
        &mut self,
        category: DiskCategory,
        added_bytes: u64,
        removed_bytes: u64,
    ) -> Result<()> {
        let current_category = self.category_bytes(category);
        let updated_category = current_category
            .checked_sub(removed_bytes)
            .and_then(|remaining| remaining.checked_add(added_bytes))
            .ok_or_else(|| {
                TsinkError::Other(format!(
                    "local disk accounting delta is outside the supported range for {category:?}: current={current_category}, added={added_bytes}, removed={removed_bytes}"
                ))
            })?;
        let other_categories = self
            .accounted_bytes()
            .checked_sub(current_category)
            .expect("disk accounting category exceeded its validated total");
        other_categories.checked_add(updated_category).ok_or_else(|| {
            TsinkError::Other(format!(
                "local disk accounting total overflow for {category:?}: other={other_categories}, category={updated_category}"
            ))
        })?;

        if updated_category == 0 {
            self.committed_by_category.remove(&category);
        } else {
            self.committed_by_category
                .insert(category, updated_category);
        }
        Ok(())
    }

    fn admit_reservation(&mut self, bytes: u64, maintenance: bool) -> Result<()> {
        let reserved_bytes = self.reserved_bytes.checked_add(bytes).ok_or_else(|| {
            TsinkError::Other(format!(
                "local disk reservation total overflow: current={}, added={bytes}",
                self.reserved_bytes
            ))
        })?;
        let maintenance_reserved_bytes = if maintenance {
            self.maintenance_reserved_bytes
                .checked_add(bytes)
                .ok_or_else(|| {
                    TsinkError::Other(format!(
                        "local disk maintenance reservation total overflow: current={}, added={bytes}",
                        self.maintenance_reserved_bytes
                    ))
                })?
        } else {
            self.maintenance_reserved_bytes
        };
        let active_reservations = self.active_reservations.checked_add(1).ok_or_else(|| {
            TsinkError::Other("local disk active reservation counter overflow".to_string())
        })?;
        if maintenance_reserved_bytes > reserved_bytes {
            return Err(TsinkError::Other(format!(
                "local disk reservation invariant violated: maintenance={maintenance_reserved_bytes}, total={reserved_bytes}"
            )));
        }

        self.reserved_bytes = reserved_bytes;
        self.maintenance_reserved_bytes = maintenance_reserved_bytes;
        self.active_reservations = active_reservations;
        Ok(())
    }

    fn grow_reservation(&mut self, bytes: u64, maintenance: bool) -> Result<()> {
        let reserved_bytes = self.reserved_bytes.checked_add(bytes).ok_or_else(|| {
            TsinkError::Other(format!(
                "local disk reservation total overflow: current={}, added={bytes}",
                self.reserved_bytes
            ))
        })?;
        let maintenance_reserved_bytes = if maintenance {
            self.maintenance_reserved_bytes
                .checked_add(bytes)
                .ok_or_else(|| {
                    TsinkError::Other(format!(
                        "local disk maintenance reservation total overflow: current={}, added={bytes}",
                        self.maintenance_reserved_bytes
                    ))
                })?
        } else {
            self.maintenance_reserved_bytes
        };
        if maintenance_reserved_bytes > reserved_bytes {
            return Err(TsinkError::Other(format!(
                "local disk reservation invariant violated: maintenance={maintenance_reserved_bytes}, total={reserved_bytes}"
            )));
        }

        self.reserved_bytes = reserved_bytes;
        self.maintenance_reserved_bytes = maintenance_reserved_bytes;
        Ok(())
    }

    fn release_reservation(&mut self, bytes: u64, maintenance: bool) -> bool {
        let reserved_bytes = self
            .reserved_bytes
            .checked_sub(bytes)
            .expect("released bytes exceeded the local disk reservation total");
        let maintenance_reserved_bytes = if maintenance {
            self.maintenance_reserved_bytes
                .checked_sub(bytes)
                .expect("released bytes exceeded the maintenance reservation total")
        } else {
            self.maintenance_reserved_bytes
        };
        let active_reservations = self
            .active_reservations
            .checked_sub(1)
            .expect("released a local disk reservation when none were active");
        assert!(
            maintenance_reserved_bytes <= reserved_bytes,
            "maintenance reservations exceeded the total after a reservation release"
        );

        self.reserved_bytes = reserved_bytes;
        self.maintenance_reserved_bytes = maintenance_reserved_bytes;
        self.active_reservations = active_reservations;
        active_reservations == 0
    }
}

/// Shared owner for one local data-directory disk envelope.
pub struct LocalDiskBudget {
    configured_root: PathBuf,
    root: PathBuf,
    limits: LocalDiskLimits,
    state: Mutex<DiskAccountingState>,
    managed_file_mutation_lock: Mutex<()>,
    reservations_released: Condvar,
    space_probe: Arc<dyn SpaceProbe>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedDirectoryEntryExpectation {
    DirectoryOrMissing,
    Missing,
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn combine_managed_directory_replacement<T>(
    operation: Result<T>,
    reconciliation: Result<()>,
) -> Result<T> {
    match (operation, reconciliation) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(err), Ok(())) => Err(err),
        (Ok(_), Err(err)) => Err(TsinkError::Other(format!(
            "managed directory replacement committed but accounting reconciliation failed: {err}"
        ))),
        (Err(operation_err), Err(reconciliation_err)) => Err(TsinkError::Other(format!(
            "managed directory replacement failed: {operation_err}; disk reconciliation failed: {reconciliation_err}"
        ))),
    }
}

impl fmt::Debug for LocalDiskBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDiskBudget")
            .field("configured_root", &self.configured_root)
            .field("root", &self.root)
            .field("limits", &self.limits)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl LocalDiskBudget {
    /// Opens a budget, creates the root when necessary, and reconciles its current contents.
    pub fn open(root: impl AsRef<Path>, limits: LocalDiskLimits) -> Result<Arc<Self>> {
        Self::open_with_space_probe_and_memory_limit(
            root.as_ref(),
            limits,
            Arc::new(SystemSpaceProbe),
            usize::MAX,
        )
    }

    /// Opens and reconciles a budget while bounding startup scan scratch by `memory_limit_bytes`.
    ///
    /// Reconciliation streams one directory entry at a time and retains only the current
    /// depth-first path. The admission limit therefore depends on the deepest path observed, not
    /// on the number of siblings beneath the managed root.
    pub(crate) fn open_with_startup_memory_limit(
        root: impl AsRef<Path>,
        limits: LocalDiskLimits,
        memory_limit_bytes: usize,
    ) -> Result<Arc<Self>> {
        Self::open_with_space_probe_and_memory_limit(
            root.as_ref(),
            limits,
            Arc::new(SystemSpaceProbe),
            memory_limit_bytes,
        )
    }

    #[cfg(test)]
    pub(crate) fn open_with_available_space_for_test(
        root: impl AsRef<Path>,
        limits: LocalDiskLimits,
        available_space: u64,
    ) -> Result<Arc<Self>> {
        Self::open_with_space_probe(
            root.as_ref(),
            limits,
            Arc::new(FixedAvailableSpaceProbeForTest(available_space)),
        )
    }

    #[cfg(test)]
    fn open_with_space_probe(
        root: &Path,
        limits: LocalDiskLimits,
        space_probe: Arc<dyn SpaceProbe>,
    ) -> Result<Arc<Self>> {
        Self::open_with_space_probe_and_memory_limit(root, limits, space_probe, usize::MAX)
    }

    fn open_with_space_probe_and_memory_limit(
        root: &Path,
        limits: LocalDiskLimits,
        space_probe: Arc<dyn SpaceProbe>,
        memory_limit_bytes: usize,
    ) -> Result<Arc<Self>> {
        let limits = limits.validate()?;
        let configured_root = absolute_path_lexically_normalized(root)?;
        fs::create_dir_all(root).map_err(|source| TsinkError::IoWithPath {
            path: root.to_path_buf(),
            source,
        })?;
        let root = fs::canonicalize(root).map_err(|source| TsinkError::IoWithPath {
            path: root.to_path_buf(),
            source,
        })?;
        sync_directory_ancestry(&root)?;
        let budget = Arc::new(Self {
            configured_root,
            root,
            limits,
            state: Mutex::new(DiskAccountingState::default()),
            managed_file_mutation_lock: Mutex::new(()),
            reservations_released: Condvar::new(),
            space_probe,
        });
        budget.reconcile_with_memory_limit(memory_limit_bytes)?;
        Ok(budget)
    }

    /// Returns the data-directory root covered by this budget.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the active logical, physical-headroom, and maintenance-reserve limits.
    pub fn limits(&self) -> LocalDiskLimits {
        self.limits
    }

    /// Returns the effective per-entry allowance used for snapshot restore staging admission.
    ///
    /// This is `max(SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES,
    /// destination_filesystem_allocation_unit)`. The copied-tree component of budgeted restore is
    /// `logical_file_bytes + entry_count * this_value`; semantic-validation recovery scratch and
    /// operation-owned entries extend the complete restore peak. The result is a conservative
    /// admission unit for entry metadata and minimum allocation, not an exact statement of total
    /// physical filesystem consumption.
    pub fn snapshot_restore_entry_staging_allowance_bytes(&self) -> Result<u64> {
        let allocation_unit = self
            .space_probe
            .allocation_unit(&self.root)
            .map_err(|source| TsinkError::IoWithPath {
                path: self.root.clone(),
                source,
            })?;
        if allocation_unit == 0 {
            return Err(TsinkError::Other(format!(
                "filesystem reported a zero-byte allocation unit for {}",
                self.root.display()
            )));
        }
        Ok(allocation_unit.max(crate::SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES))
    }

    /// Returns whether `path` resolves to the managed root or one of its descendants.
    ///
    /// Existing symlinks are resolved component by component. Missing final components are
    /// normalized lexically, so this remains safe for destinations that have not been created.
    pub fn governs(&self, path: &Path) -> Result<bool> {
        Ok(resolve_path_allow_missing(path)?.starts_with(&self.root))
    }

    /// Returns whether `path` overlaps the managed root in either direction.
    ///
    /// This is useful for independent persistence roots, such as an object store, that must be
    /// neither inside the quota tree nor an ancestor of it. Both lexical configured paths and
    /// resolved paths are checked so an in-tree symlink to an external directory is still rejected
    /// as an overlapping persistence namespace.
    pub fn overlaps(&self, path: &Path) -> Result<bool> {
        let lexical = absolute_path_lexically_normalized(path)?;
        if lexical.starts_with(&self.configured_root)
            || self.configured_root.starts_with(&lexical)
            || lexical.starts_with(&self.root)
            || self.root.starts_with(&lexical)
        {
            return Ok(true);
        }
        let resolved = resolve_path_allow_missing(path)?;
        Ok(resolved.starts_with(&self.root) || self.root.starts_with(resolved))
    }

    /// Returns whether the directory entry at `path` belongs to the managed tree.
    ///
    /// Unlike [`LocalDiskBudget::governs`], this resolves the parent but deliberately does not
    /// follow the final component. Filesystem mutators use this because replacing or removing an
    /// in-tree symlink mutates the managed directory entry even when its target is outside. A path
    /// lexically beneath the configured or canonical root that escapes through an intermediate
    /// symlink is rejected instead of being mistaken for an intentionally external path.
    pub(crate) fn governs_entry(&self, path: &Path) -> Result<bool> {
        let Some(file_name) = path.file_name() else {
            return self.matches_root(path);
        };
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let governed = resolve_path_allow_missing(parent)?
            .join(file_name)
            .starts_with(&self.root);
        if !governed {
            let lexical = absolute_path_lexically_normalized(path)?;
            if lexical.starts_with(&self.configured_root) || lexical.starts_with(&self.root) {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "managed path escapes local disk root {} through an existing filesystem entry: {}",
                    self.root.display(),
                    path.display()
                )));
            }
        }
        Ok(governed)
    }

    /// Validates an owned regular-file path without following its final component.
    ///
    /// Missing files are allowed. Existing symlinks and non-file entries are rejected so a
    /// sidecar cannot load or append through an entry that reconciliation deliberately does not
    /// follow.
    pub fn validate_managed_file_path(&self, path: &Path) -> Result<()> {
        if !self.governs_entry(path)? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed file is outside local disk root {}: {}",
                self.root.display(),
                path.display()
            )));
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_file() => Ok(()),
            Ok(metadata) => Err(TsinkError::InvalidConfiguration(format!(
                "managed file must be a regular file, found {:?}: {}",
                metadata.file_type(),
                path.display()
            ))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            }),
        }
    }

    /// Validates an owned directory path without following its final component.
    ///
    /// Missing directories are allowed. Existing symlinks and non-directory entries are rejected
    /// so startup cannot follow an owned namespace outside the process-locked data tree.
    pub(crate) fn validate_managed_directory_path(&self, path: &Path) -> Result<()> {
        if !self.governs_entry(path)? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed directory is outside local disk root {}: {}",
                self.root.display(),
                path.display()
            )));
        }
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if !crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    && metadata.file_type().is_dir() =>
            {
                Ok(())
            }
            Ok(metadata) => Err(TsinkError::InvalidConfiguration(format!(
                "managed directory must be a directory, found {:?}: {}",
                metadata.file_type(),
                path.display()
            ))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            }),
        }
    }

    /// Runs one staged managed-directory replacement under a serialized peak reservation.
    ///
    /// `target`, `staging`, and `backup` must be distinct, non-overlapping strict descendants of
    /// this budget's root. Existing symlinks in any destination component are rejected before the
    /// reservation is admitted or `replace` is invoked. `staging` and `backup` must not exist;
    /// `target` may be an existing directory or a missing entry.
    ///
    /// The caller's conservative staging admission (logical payload plus snapshot-entry allowance)
    /// is extended by `entry_allowance_bytes` for every missing target-parent directory, then
    /// reserved before the closure can create directories. On every ordinary closure return, the
    /// reservation is released through a static link-aware scan. A successful scan replaces the
    /// committed totals exactly before new admission resumes. A scan failure is explicit and
    /// conservatively charges the full reservation instead of claiming exact accounting.
    pub(crate) fn with_managed_directory_replacement<T, F>(
        self: &Arc<Self>,
        target: &Path,
        staging: &Path,
        backup: &Path,
        staging_admission_bytes: u64,
        entry_allowance_bytes: u64,
        replace: F,
    ) -> Result<T>
    where
        F: FnOnce(&Path, &Path, &Path) -> Result<T>,
    {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        let target = self.validate_strict_descendant_directory_entry(
            target,
            ManagedDirectoryEntryExpectation::DirectoryOrMissing,
            "restore target",
        )?;
        let staging = self.validate_strict_descendant_directory_entry(
            staging,
            ManagedDirectoryEntryExpectation::Missing,
            "restore staging path",
        )?;
        let backup = self.validate_strict_descendant_directory_entry(
            backup,
            ManagedDirectoryEntryExpectation::Missing,
            "restore backup path",
        )?;

        let paths = [&target, &staging, &backup];
        for (index, path) in paths.iter().enumerate() {
            for other in paths.iter().skip(index + 1) {
                if paths_overlap(path, other) {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "managed restore paths overlap: {} and {}",
                        path.display(),
                        other.display()
                    )));
                }
            }
        }

        let missing_target_parent_entries =
            self.count_missing_target_parent_directories(&target)?;
        let target_parent_admission_bytes = missing_target_parent_entries
            .checked_mul(entry_allowance_bytes)
            .ok_or_else(|| {
                TsinkError::Other(
                    "restore target-parent staging allowance exceeds the supported byte range"
                        .to_string(),
                )
            })?;
        let staging_admission_bytes = staging_admission_bytes
            .checked_add(target_parent_admission_bytes)
            .ok_or_else(|| {
                TsinkError::Other(
                    "restore staging admission exceeds the supported byte range".to_string(),
                )
            })?;

        let reservation = self.reserve(
            DiskCategory::Temporary,
            staging_admission_bytes,
            DiskReservationKind::Growth,
        )?;
        let operation_result = replace(&target, &staging, &backup);
        let reconciliation_result = reservation.finish_by_reconciling();
        combine_managed_directory_replacement(operation_result, reconciliation_result)
    }

    fn count_missing_target_parent_directories(&self, target: &Path) -> Result<u64> {
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "restore target has no parent directory: {}",
                target.display()
            ))
        })?;
        let relative = parent.strip_prefix(&self.root).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "restore target parent is outside local disk root {}: {}",
                self.root.display(),
                parent.display()
            ))
        })?;
        let parent_depth = relative.components().count();
        if parent_depth > crate::MAX_SNAPSHOT_RESTORE_DEPTH as usize {
            return Err(TsinkError::InvalidConfiguration(format!(
                "restore target parent depth {parent_depth} exceeds limit {}: {}",
                crate::MAX_SNAPSHOT_RESTORE_DEPTH,
                parent.display()
            )));
        }

        let mut current = self.root.clone();
        let mut missing = 0u64;
        for component in relative.components() {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if crate::engine::fs_utils::is_link_or_reparse_point(&metadata) => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "restore target parent contains a link-like entry: {}",
                        current.display()
                    )))
                }
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(metadata) => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "restore target parent contains {:?} instead of a directory: {}",
                        metadata.file_type(),
                        current.display()
                    )))
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    missing = missing.checked_add(1).ok_or_else(|| {
                        TsinkError::Other(
                            "restore target missing-parent count exceeds the supported range"
                                .to_string(),
                        )
                    })?;
                }
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: current,
                        source,
                    })
                }
            }
        }
        Ok(missing)
    }

    fn validate_strict_descendant_directory_entry(
        &self,
        path: &Path,
        expectation: ManagedDirectoryEntryExpectation,
        role: &str,
    ) -> Result<PathBuf> {
        let lexical = absolute_path_lexically_normalized(path)?;
        let (lexical_root, relative) = if let Ok(relative) = lexical.strip_prefix(&self.root) {
            (&self.root, relative)
        } else if let Ok(relative) = lexical.strip_prefix(&self.configured_root) {
            (&self.configured_root, relative)
        } else {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{role} must be a strict descendant of local disk root {}: {}",
                self.root.display(),
                path.display()
            )));
        };
        if relative.as_os_str().is_empty() {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{role} must be a strict descendant of local disk root {}, not the root itself",
                self.root.display()
            )));
        }

        let mut current = lexical_root.clone();
        let component_count = relative.components().count();
        for (index, component) in relative.components().enumerate() {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if crate::engine::fs_utils::is_link_or_reparse_point(&metadata) => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "{role} contains an existing symlink component: {}",
                        current.display()
                    )))
                }
                Ok(metadata) if index + 1 < component_count && !metadata.file_type().is_dir() => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "{role} contains a non-directory component: {}",
                        current.display()
                    )))
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: current,
                        source,
                    })
                }
            }
        }

        let resolved = resolve_entry_path_no_follow(&lexical)?;
        if resolved == self.root || !resolved.starts_with(&self.root) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{role} must resolve to a strict descendant of local disk root {}: {}",
                self.root.display(),
                path.display()
            )));
        }
        match fs::symlink_metadata(&resolved) {
            Ok(metadata)
                if expectation == ManagedDirectoryEntryExpectation::DirectoryOrMissing
                    && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    && metadata.file_type().is_dir() =>
            {
                Ok(resolved)
            }
            Ok(metadata) if expectation == ManagedDirectoryEntryExpectation::Missing => {
                Err(TsinkError::InvalidConfiguration(format!(
                    "{role} must not already exist, found {:?}: {}",
                    metadata.file_type(),
                    resolved.display()
                )))
            }
            Ok(metadata) => Err(TsinkError::InvalidConfiguration(format!(
                "{role} must be a directory, found {:?}: {}",
                metadata.file_type(),
                resolved.display()
            ))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(resolved),
            Err(source) => Err(TsinkError::IoWithPath {
                path: resolved,
                source,
            }),
        }
    }

    /// Creates a managed directory tree and synchronizes every newly-created parent entry.
    pub fn create_dir_all_and_sync_parents(&self, directory: &Path) -> Result<()> {
        let directory = resolve_path_allow_missing(directory)?;
        if !directory.starts_with(&self.root) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed directory is outside local disk root {}: {}",
                self.root.display(),
                directory.display()
            )));
        }

        fs::create_dir_all(&directory).map_err(|source| TsinkError::IoWithPath {
            path: directory.clone(),
            source,
        })?;

        self.sync_managed_directory_ancestry(&directory)
    }

    fn sync_managed_directory_ancestry(&self, directory: &Path) -> Result<()> {
        if !directory.starts_with(&self.root) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed directory is outside local disk root {}: {}",
                self.root.display(),
                directory.display()
            )));
        }

        // Retry every ancestor sync even when a previous attempt created the directories before
        // failing. Otherwise the next startup could observe the entries, skip all syncs, and
        // report success while their parent links still are not crash-durable.
        crate::engine::fs_utils::sync_parent_dir(&self.root)?;
        let relative = directory.strip_prefix(&self.root).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "managed directory is outside local disk root {}: {}",
                self.root.display(),
                directory.display()
            ))
        })?;
        let mut current = self.root.clone();
        for component in relative.components() {
            current.push(component.as_os_str());
            let metadata =
                fs::symlink_metadata(&current).map_err(|source| TsinkError::IoWithPath {
                    path: current.clone(),
                    source,
                })?;
            if !metadata.file_type().is_dir() {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "managed directory path contains a non-directory entry: {}",
                    current.display()
                )));
            }
            crate::engine::fs_utils::sync_parent_dir(&current)?;
        }
        Ok(())
    }

    /// Removes orphan temporary files created by atomic replacement of `target`.
    ///
    /// Call this while holding the data-path process lease, before opening the owned store. Only
    /// regular files or symlinks with the exact generated `.<target>.tmp-<pid>-<nonce>` shape are
    /// removed; directories or other entry types cause startup to fail rather than deleting
    /// ambiguous host data.
    pub fn cleanup_atomic_write_temps(self: &Arc<Self>, target: &Path) -> Result<u64> {
        self.cleanup_atomic_write_temps_with_startup_memory_limit(target, usize::MAX)
    }

    pub(crate) fn cleanup_atomic_write_temps_with_startup_memory_limit(
        self: &Arc<Self>,
        target: &Path,
        memory_limit_bytes: usize,
    ) -> Result<u64> {
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed file has no parent directory: {}",
                target.display()
            ))
        })?;
        if !self.governs_entry(target)? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed file is outside local disk root {}: {}",
                self.root.display(),
                target.display()
            )));
        }
        let file_name = target.file_name().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed file has no final component: {}",
                target.display()
            ))
        })?;
        let target_name = file_name.to_str().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed file name is not valid UTF-8: {}",
                target.display()
            ))
        })?;
        self.cleanup_owned_temporary_entries_matching(
            parent,
            false,
            |candidate| atomic_write_temp_target_name(candidate) == Some(target_name),
            "atomic-write temporary cleanup",
            memory_limit_bytes,
            true,
        )
    }

    pub(crate) fn preflight_atomic_write_temps_with_startup_memory_limit(
        self: &Arc<Self>,
        target: &Path,
        memory_limit_bytes: usize,
    ) -> Result<u64> {
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed file has no parent directory: {}",
                target.display()
            ))
        })?;
        if !self.governs_entry(target)? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed file is outside local disk root {}: {}",
                self.root.display(),
                target.display()
            )));
        }
        let target_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "managed file name is not valid UTF-8: {}",
                    target.display()
                ))
            })?;
        self.cleanup_owned_temporary_entries_matching(
            parent,
            false,
            |candidate| atomic_write_temp_target_name(candidate) == Some(target_name),
            "atomic-write temporary cleanup preflight",
            memory_limit_bytes,
            false,
        )
    }

    /// Removes one explicitly owned managed file and synchronizes its parent directory.
    ///
    /// Missing paths are accepted as a no-op without rescanning the tree. Regular files and
    /// symlinks are unlinked without following the final component; directories and special file
    /// types are rejected. After an actual removal, accounting is reconciled exactly before return.
    pub fn remove_managed_file_if_exists_and_sync_parent(
        self: &Arc<Self>,
        path: &Path,
        category: DiskCategory,
    ) -> Result<()> {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        if !self.governs_entry(path)? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed file is outside local disk root {}: {}",
                self.root.display(),
                path.display()
            )));
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_file() || metadata.file_type().is_symlink() => {
            }
            Ok(metadata) => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "managed file cleanup found unsupported entry type {:?}: {}",
                    metadata.file_type(),
                    path.display()
                )))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: path.to_path_buf(),
                    source,
                })
            }
        }
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
            path,
            Some(self),
            category,
        )
    }

    /// Removes generated atomic-write temporaries whose final target name is explicitly owned by
    /// the caller. The directory is not created when absent.
    pub(crate) fn cleanup_atomic_write_temps_matching_targets_with_startup_memory_limit<F>(
        self: &Arc<Self>,
        directory: &Path,
        owns_target: F,
        memory_limit_bytes: usize,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching(
            directory,
            false,
            |candidate| atomic_write_temp_target_name(candidate).is_some_and(&owns_target),
            "owned atomic-write temporary cleanup",
            memory_limit_bytes,
            true,
        )
    }

    pub(crate) fn preflight_atomic_write_temps_matching_targets_with_startup_memory_limit<F>(
        self: &Arc<Self>,
        directory: &Path,
        owns_target: F,
        memory_limit_bytes: usize,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching(
            directory,
            false,
            |candidate| atomic_write_temp_target_name(candidate).is_some_and(&owns_target),
            "owned atomic-write temporary cleanup preflight",
            memory_limit_bytes,
            false,
        )
    }

    /// Removes explicitly-owned temporary directory entries. Regular files and symlinks with an
    /// owned name are also safe to unlink; special file types are rejected.
    pub(crate) fn cleanup_temporary_directories_matching_names_with_startup_memory_limit<F>(
        self: &Arc<Self>,
        directory: &Path,
        owns_name: F,
        memory_limit_bytes: usize,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching(
            directory,
            true,
            owns_name,
            "owned temporary-directory cleanup",
            memory_limit_bytes,
            true,
        )
    }

    pub(crate) fn preflight_temporary_directories_matching_names_with_startup_memory_limit<F>(
        self: &Arc<Self>,
        directory: &Path,
        owns_name: F,
        memory_limit_bytes: usize,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching(
            directory,
            true,
            owns_name,
            "owned temporary-directory cleanup preflight",
            memory_limit_bytes,
            false,
        )
    }

    fn cleanup_owned_temporary_entries_matching<F>(
        self: &Arc<Self>,
        directory: &Path,
        allow_directories: bool,
        owns_name: F,
        operation: &str,
        memory_limit_bytes: usize,
        execute: bool,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching_with_namespace_limits(
            directory,
            allow_directories,
            owns_name,
            operation,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
            memory_limit_bytes,
            execute,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn cleanup_owned_temporary_entries_matching_with_namespace_limits<F>(
        self: &Arc<Self>,
        directory: &Path,
        allow_directories: bool,
        owns_name: F,
        operation: &str,
        max_directory_entries: usize,
        max_recursive_entries: usize,
        max_recursive_depth: u32,
        memory_limit_bytes: usize,
        execute: bool,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        self.validate_managed_directory_path(directory)?;
        if !crate::engine::fs_utils::path_exists_no_follow(directory)? {
            return Ok(0);
        }

        let mut directory_budget =
            crate::engine::fs_utils::RecoveryNamespaceBudget::new(max_directory_entries);
        let mut owned_directories = Vec::new();
        let mut owned_file_like_entries = Vec::new();
        admit_startup_memory(
            memory_limit_bytes,
            modeled_path_vectors_bytes(&owned_directories, &owned_file_like_entries)?
                .checked_add(std::mem::size_of::<fs::ReadDir>())
                .ok_or_else(|| {
                    TsinkError::Other(format!("{operation} directory memory model overflow"))
                })?,
        )?;
        let entries = fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        for entry in entries {
            directory_budget.observe_entry(directory, operation)?;
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: directory.to_path_buf(),
                source,
            })?;
            let entry_name = entry.file_name();
            let component_bytes = entry_name.as_encoded_bytes().len();
            let anticipated_path_bytes = directory
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .checked_add(component_bytes)
                .and_then(|bytes| bytes.checked_add(1))
                .ok_or_else(|| {
                    TsinkError::Other(format!("{operation} path-size model overflow"))
                })?;
            let transient_required =
                modeled_path_vectors_bytes(&owned_directories, &owned_file_like_entries)?
                    .checked_add(std::mem::size_of::<fs::ReadDir>())
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<fs::DirEntry>()))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
                    .and_then(|bytes| bytes.checked_add(component_bytes))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
                    .and_then(|bytes| bytes.checked_add(anticipated_path_bytes))
                    .ok_or_else(|| {
                        TsinkError::Other(format!("{operation} memory model overflow"))
                    })?;
            admit_startup_memory(memory_limit_bytes, transient_required)?;
            let Some(entry_name) = entry_name.to_str() else {
                continue;
            };
            if !owns_name(entry_name) {
                continue;
            }
            let entry_path = entry.path();
            let metadata =
                fs::symlink_metadata(&entry_path).map_err(|source| TsinkError::IoWithPath {
                    path: entry_path.clone(),
                    source,
                })?;
            let file_type = metadata.file_type();
            let is_link_like = crate::engine::fs_utils::is_link_or_reparse_point(&metadata);
            if !(file_type.is_file() || is_link_like || (file_type.is_dir() && allow_directories)) {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "refusing to remove ambiguous owned temporary entry: {}",
                    entry_path.display()
                )));
            }
            if file_type.is_dir() && !is_link_like {
                push_modeled_cleanup_path(
                    &mut owned_directories,
                    entry_path,
                    &owned_file_like_entries,
                    memory_limit_bytes,
                    operation,
                )?;
            } else {
                push_modeled_cleanup_path(
                    &mut owned_file_like_entries,
                    entry_path,
                    &owned_directories,
                    memory_limit_bytes,
                    operation,
                )?;
            }
        }
        if owned_directories.is_empty() && owned_file_like_entries.is_empty() {
            return Ok(0);
        }

        let retained_root_bytes =
            modeled_path_vectors_bytes(&owned_directories, &owned_file_like_entries)?;
        let mut recursive_budget =
            crate::engine::fs_utils::RecoveryNamespaceBudget::new(max_recursive_entries);
        let removal_plan = crate::engine::fs_utils::validate_recursive_namespace_with_admission(
            &owned_directories,
            &mut recursive_budget,
            max_recursive_depth,
            operation,
            retained_root_bytes,
            |required| admit_startup_memory(memory_limit_bytes, required),
        )?
        .include_file_like_roots_with_admission(
            &owned_file_like_entries,
            retained_root_bytes,
            |required| admit_startup_memory(memory_limit_bytes, required),
        )?;

        if !execute {
            return Ok(
                u64::try_from(owned_directories.len() + owned_file_like_entries.len())
                    .unwrap_or(u64::MAX),
            );
        }

        let reservation =
            self.reserve(DiskCategory::Temporary, 0, DiskReservationKind::Recovery)?;
        let removal_result = removal_plan.remove();
        let mut cleanup_error = removal_result.as_ref().err().map(ToString::to_string);
        if let Err(err) = crate::engine::fs_utils::sync_dir(directory) {
            cleanup_error.get_or_insert_with(|| err.to_string());
        }
        let removed = removal_result
            .as_ref()
            .ok()
            .map(|removed| u64::try_from(*removed).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let settlement_result = reservation.commit(0, 0);
        let reconciliation_result = self
            .reconcile_when_idle_with_memory_limit(memory_limit_bytes)
            .map(|_| ());
        match (cleanup_error, settlement_result, reconciliation_result) {
            (None, Ok(()), Ok(())) => Ok(removed),
            (cleanup_error, settlement, reconciliation) => {
                let mut errors = Vec::new();
                if let Some(err) = cleanup_error {
                    errors.push(format!("cleanup failed: {err}"));
                }
                if let Err(err) = settlement {
                    errors.push(format!("disk settlement failed: {err}"));
                }
                if let Err(err) = reconciliation {
                    errors.push(format!("disk reconciliation failed: {err}"));
                }
                Err(TsinkError::Other(format!(
                    "{operation} for {} failed: {}",
                    directory.display(),
                    errors.join("; ")
                )))
            }
        }
    }

    /// Stages a group of managed replacements under one growth reservation and coordinator lock.
    ///
    /// The complete temporary peak (the checked sum of all replacement lengths plus one
    /// allocation-unit allowance for each staged temporary entry and each missing parent
    /// directory) is admitted before the filesystem is mutated. Every owned temporary is then
    /// fully written, flushed, and synchronized before `publish` is invoked. The closure chooses
    /// publication order with [`StagedManagedFileReplacements::publish`]. On every ordinary
    /// return, unpublished temporaries are removed and any visible or ambiguous mutation is
    /// reconciled against an exact tree scan before this method returns.
    ///
    /// The closure must publish every replacement before returning success. It may inspect
    /// [`StagedManagedFileReplacements::is_published`] after an error to distinguish a failed
    /// rename from a parent-directory sync failure after publication.
    pub fn with_staged_managed_file_replacements<T, F>(
        self: &Arc<Self>,
        replacements: &[ManagedFileReplacement<'_>],
        category: DiskCategory,
        publish: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut StagedManagedFileReplacements) -> Result<T>,
    {
        self.with_staged_managed_file_replacements_with_kind(
            replacements,
            category,
            DiskReservationKind::Growth,
            false,
            publish,
        )
    }

    /// Stages a combined non-growing managed rewrite with recovery admission.
    ///
    /// Recovery work may proceed while reconciled usage exceeds the logical quota, but the sum
    /// of replacement lengths must not exceed the sum of the current target lengths. The complete
    /// temporary peak is still reserved against the physical free-space floor. This is intended
    /// for checkpoint repair and compaction where the group as a whole releases or preserves
    /// persistent space.
    pub fn with_staged_managed_file_replacements_for_recovery<T, F>(
        self: &Arc<Self>,
        replacements: &[ManagedFileReplacement<'_>],
        category: DiskCategory,
        publish: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut StagedManagedFileReplacements) -> Result<T>,
    {
        self.with_staged_managed_file_replacements_with_kind(
            replacements,
            category,
            DiskReservationKind::Recovery,
            true,
            publish,
        )
    }

    /// Stages repair of an already-authoritative managed representation with recovery admission.
    ///
    /// Unlike [`LocalDiskBudget::with_staged_managed_file_replacements_for_recovery`], this does
    /// not require the replacement group to be non-growing. It is only correct when another
    /// already-durable representation (for example, an authoritative consensus log checkpoint)
    /// proves that these bytes are existing logical state rather than newly-admitted growth. The
    /// full temporary peak still reserves physical space and preserves the filesystem headroom.
    pub fn with_staged_managed_file_replacements_for_authoritative_recovery<T, F>(
        self: &Arc<Self>,
        replacements: &[ManagedFileReplacement<'_>],
        category: DiskCategory,
        publish: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut StagedManagedFileReplacements) -> Result<T>,
    {
        self.with_staged_managed_file_replacements_with_kind(
            replacements,
            category,
            DiskReservationKind::Recovery,
            false,
            publish,
        )
    }

    fn with_staged_managed_file_replacements_with_kind<T, F>(
        self: &Arc<Self>,
        replacements: &[ManagedFileReplacement<'_>],
        category: DiskCategory,
        kind: DiskReservationKind,
        require_non_growing: bool,
        publish: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut StagedManagedFileReplacements) -> Result<T>,
    {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        let mut prepared = Vec::with_capacity(replacements.len());
        let mut distinct_targets = BTreeSet::new();
        let mut distinct_parents = BTreeSet::new();
        let mut missing_parent_directories = BTreeSet::new();
        let mut replacement_bytes = 0u64;
        let mut previous_bytes = 0u64;

        for replacement in replacements {
            let target = resolve_entry_path_no_follow(replacement.target)?;
            if !distinct_targets.insert(target.clone()) {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "grouped managed replacement contains duplicate target {}",
                    target.display()
                )));
            }
            let parent = target.parent().ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "grouped managed replacement target has no parent directory: {}",
                    target.display()
                ))
            })?;
            self.validate_managed_file_path(&target)?;
            self.collect_missing_managed_parent_directories(
                &target,
                &mut missing_parent_directories,
            )?;
            distinct_parents.insert(parent.to_path_buf());

            let encoded_bytes = u64::try_from(replacement.bytes.len()).map_err(|_| {
                TsinkError::Other(format!(
                    "grouped replacement for {} exceeds the supported byte range",
                    target.display()
                ))
            })?;
            replacement_bytes = replacement_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "grouped managed replacement temporary peak exceeds the supported byte range"
                            .to_string(),
                    )
                })?;
            previous_bytes = previous_bytes
                .checked_add(measured_path_bytes(&target)?)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "grouped managed replacement current size exceeds the supported byte range"
                            .to_string(),
                    )
                })?;
            prepared.push((target, replacement.bytes));
        }

        if require_non_growing && replacement_bytes > previous_bytes {
            return Err(TsinkError::InvalidConfiguration(format!(
                "recovery rewrite would grow grouped managed state from {previous_bytes} to {replacement_bytes} bytes"
            )));
        }

        let staged_entry_count = u64::try_from(prepared.len()).map_err(|_| {
            TsinkError::Other(
                "grouped managed replacement count exceeds the supported range".to_string(),
            )
        })?;
        let missing_parent_count =
            u64::try_from(missing_parent_directories.len()).map_err(|_| {
                TsinkError::Other(
                    "grouped managed replacement missing-parent count exceeds the supported range"
                        .to_string(),
                )
            })?;
        let staged_entry_allowance_bytes = if staged_entry_count == 0 && missing_parent_count == 0 {
            0
        } else {
            staged_entry_count
                .checked_add(missing_parent_count)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "grouped managed replacement staged-entry count exceeds the supported range"
                            .to_string(),
                    )
                })?
                .checked_mul(self.snapshot_restore_entry_staging_allowance_bytes()?)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "grouped managed replacement staged-entry allowance exceeds the supported byte range"
                            .to_string(),
                    )
                })?
        };
        let staging_admission_bytes = replacement_bytes
            .checked_add(staged_entry_allowance_bytes)
            .ok_or_else(|| {
                TsinkError::Other(
                    "grouped managed replacement staging admission exceeds the supported byte range"
                        .to_string(),
                )
            })?;

        let reservation = self.reserve(category, staging_admission_bytes, kind)?;
        let mut staged = StagedManagedFileReplacements {
            budget: Some(Arc::clone(self)),
            category: Some(category),
            reservation: Some(reservation),
            replacements: Vec::with_capacity(prepared.len()),
            created_directories: Vec::new(),
            publication_ambiguous: false,
            finalized: false,
        };

        // From the first filesystem mutation until every temporary has been created, ownership
        // and durability are still in flight. Keep the reservation conservatively reconciling
        // during this window so unwind cannot release unscanned bytes.
        staged.publication_ambiguous = true;
        if let Err(stage_err) = self.create_missing_managed_parent_directories(
            &missing_parent_directories,
            &mut staged.created_directories,
        ) {
            let finalization_result = staged.finalize();
            return combine_grouped_replacement_operation(Err(stage_err), finalization_result);
        }
        for parent in distinct_parents {
            if let Err(stage_err) = self.sync_managed_directory_ancestry(&parent) {
                let finalization_result = staged.finalize();
                return combine_grouped_replacement_operation(Err(stage_err), finalization_result);
            }
        }

        for (target, bytes) in prepared {
            match crate::engine::fs_utils::write_tmp_and_sync_with_observer(
                &target,
                bytes,
                |temporary| {
                    staged.replacements.push(StagedManagedFileReplacement {
                        target: target.clone(),
                        temporary: temporary.to_path_buf(),
                        published: false,
                    });
                },
            ) {
                Ok(_) => {}
                Err(stage_err) => {
                    // The creation observer transferred cleanup ownership before any payload
                    // write. Keep the staging window conservative until finalization nevertheless,
                    // so cleanup or directory-sync failures are replaced by an exact scan.
                    let finalization_result = staged.finalize();
                    return combine_grouped_replacement_operation(
                        Err(stage_err),
                        finalization_result,
                    );
                }
            }
        }
        staged.publication_ambiguous = false;

        let mut operation_result = publish(&mut staged);
        if operation_result.is_ok() && staged.published_count() != staged.len() {
            operation_result = Err(TsinkError::InvalidConfiguration(format!(
                "grouped managed replacement closure published {} of {} staged targets",
                staged.published_count(),
                staged.len()
            )));
        }
        let finalization_result = staged.finalize();
        combine_grouped_replacement_operation(operation_result, finalization_result)
    }

    fn create_missing_managed_parent_directories(
        &self,
        missing: &BTreeSet<PathBuf>,
        created: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let mut ordered = missing.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            left.components()
                .count()
                .cmp(&right.components().count())
                .then_with(|| left.cmp(right))
        });

        for directory in ordered {
            match fs::create_dir(directory) {
                Ok(()) => created.push(directory.clone()),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(directory).map_err(|source| {
                        TsinkError::IoWithPath {
                            path: directory.clone(),
                            source,
                        }
                    })?;
                    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                        || !metadata.file_type().is_dir()
                    {
                        return Err(TsinkError::InvalidConfiguration(format!(
                            "grouped managed replacement parent became {:?} instead of a directory: {}",
                            metadata.file_type(),
                            directory.display()
                        )));
                    }
                }
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: directory.clone(),
                        source,
                    })
                }
            }
        }
        Ok(())
    }

    fn collect_missing_managed_parent_directories(
        &self,
        target: &Path,
        missing: &mut BTreeSet<PathBuf>,
    ) -> Result<()> {
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "grouped managed replacement target has no parent directory: {}",
                target.display()
            ))
        })?;
        let relative = parent.strip_prefix(&self.root).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "grouped managed replacement parent is outside local disk root {}: {}",
                self.root.display(),
                parent.display()
            ))
        })?;

        let mut current = self.root.clone();
        for component in relative.components() {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if crate::engine::fs_utils::is_link_or_reparse_point(&metadata) => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "grouped managed replacement parent contains a link-like entry: {}",
                        current.display()
                    )))
                }
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(metadata) => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                    "grouped managed replacement parent contains {:?} instead of a directory: {}",
                    metadata.file_type(),
                    current.display()
                )))
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    missing.insert(current.clone());
                }
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: current,
                        source,
                    })
                }
            }
        }
        Ok(())
    }

    /// Counts distinct missing parent-directory entries for managed staging destinations.
    ///
    /// This is a read-only preflight. Callers add one filesystem-entry allowance for every
    /// returned directory before an outer operation-level reservation is admitted.
    pub(crate) fn missing_managed_parent_directory_count(
        &self,
        targets: &[PathBuf],
    ) -> Result<u64> {
        let mut missing = BTreeSet::new();
        for target in targets {
            let target = resolve_entry_path_no_follow(target)?;
            if !self.governs_entry(&target)? {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "managed staging target is outside local disk root {}: {}",
                    self.root.display(),
                    target.display()
                )));
            }
            self.collect_missing_managed_parent_directories(&target, &mut missing)?;
        }
        u64::try_from(missing.len()).map_err(|_| {
            TsinkError::Other(
                "managed staging missing-parent count exceeds the supported range".to_string(),
            )
        })
    }

    /// Atomically replaces one managed file using normal-growth admission.
    ///
    /// The complete encoded replacement is reserved as peak additional space, newly-created
    /// directory entries plus the file are synchronized, and overwrite accounting is reconciled
    /// exactly before return. If publication or settlement reports an error after rename, the
    /// previous file is restored with recovery admission before the error is returned.
    pub fn write_file_atomically_and_sync_parent(
        self: &Arc<Self>,
        path: &Path,
        bytes: &[u8],
        category: DiskCategory,
    ) -> Result<()> {
        self.write_file_atomically_and_sync_parent_with_kind(
            path,
            bytes,
            category,
            DiskReservationKind::Growth,
            false,
        )
    }

    /// Atomically rewrites one managed file during recovery or cleanup.
    ///
    /// The replacement must not be larger than the existing file. This allows required cleanup to
    /// proceed while reconciled usage is already above the logical quota, while still reserving the
    /// temporary replacement against the physical free-space floor.
    pub fn rewrite_file_atomically_and_sync_parent_for_recovery(
        self: &Arc<Self>,
        path: &Path,
        bytes: &[u8],
        category: DiskCategory,
    ) -> Result<()> {
        self.write_file_atomically_and_sync_parent_with_kind(
            path,
            bytes,
            category,
            DiskReservationKind::Recovery,
            true,
        )
    }

    /// Streams an exact-length atomic replacement for logically equivalent cleanup state.
    ///
    /// A non-growing replacement uses Recovery admission, so it can release space while the root
    /// is already above its logical quota. A replacement that grows because legacy records gain
    /// explicit fields uses normal Growth admission. The callback is bounded to `new_bytes` and
    /// can therefore encode a large replacement one record at a time.
    ///
    /// This is for compaction-like rewrites where both the old and new file represent the same
    /// logical state. If parent synchronization fails after rename, the replacement can remain
    /// visible and the method returns an error after reconciling exact on-disk accounting.
    pub fn rewrite_file_atomically_and_sync_parent_for_cleanup_with<F>(
        self: &Arc<Self>,
        path: &Path,
        new_bytes: u64,
        category: DiskCategory,
        write_replacement: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        let parent = path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed cleanup file has no parent directory: {}",
                path.display()
            ))
        })?;
        self.create_dir_all_and_sync_parents(parent)?;
        self.validate_managed_file_path(path)?;
        let previous_bytes = fs::metadata(path)
            .map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?
            .len();
        let kind = if new_bytes <= previous_bytes {
            DiskReservationKind::Recovery
        } else {
            DiskReservationKind::Growth
        };
        let reservation = self.reserve(category, new_bytes, kind)?;
        let write_result = crate::engine::fs_utils::write_file_atomically_and_sync_parent_with(
            path,
            new_bytes,
            write_replacement,
        );

        match write_result {
            Ok(()) => {
                let settlement_result = reservation.commit(new_bytes, 0);
                let reconciliation_result = self.reconcile_when_idle().map(|_| ());
                match (settlement_result, reconciliation_result) {
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
                    (Err(settlement_err), Err(reconciliation_err)) => {
                        Err(TsinkError::Other(format!(
                            "streamed cleanup settlement failed: {settlement_err}; reconciliation failed: {reconciliation_err}"
                        )))
                    }
                }
            }
            Err(write_err) => {
                // The unique temporary or the published target may survive an error. Charge the
                // admitted peak conservatively, then replace it with an exact tree scan.
                let settlement_result =
                    reservation.commit_as(DiskCategory::Temporary, new_bytes, 0);
                let reconciliation_result = self.reconcile_when_idle().map(|_| ());
                match (settlement_result, reconciliation_result) {
                    (Ok(()), Ok(())) => Err(write_err),
                    (settlement, reconciliation) => {
                        let mut errors = vec![format!("rewrite failed: {write_err}")];
                        if let Err(err) = settlement {
                            errors.push(format!("disk settlement failed: {err}"));
                        }
                        if let Err(err) = reconciliation {
                            errors.push(format!("disk reconciliation failed: {err}"));
                        }
                        Err(TsinkError::Other(format!(
                            "streamed cleanup rewrite of {} failed: {}",
                            path.display(),
                            errors.join("; ")
                        )))
                    }
                }
            }
        }
    }

    fn write_file_atomically_and_sync_parent_with_kind(
        self: &Arc<Self>,
        path: &Path,
        bytes: &[u8],
        category: DiskCategory,
        kind: DiskReservationKind,
        require_non_growing: bool,
    ) -> Result<()> {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        let parent = path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed file has no parent directory: {}",
                path.display()
            ))
        })?;
        self.create_dir_all_and_sync_parents(parent)?;
        self.validate_managed_file_path(path)?;
        let previous = match fs::symlink_metadata(path) {
            Ok(_) => Some(fs::read(path).map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: path.to_path_buf(),
                    source,
                })
            }
        };
        if require_non_growing {
            let previous_bytes = previous.as_ref().map_or(0, Vec::len);
            if bytes.len() > previous_bytes {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "recovery rewrite for {} would grow managed state from {} to {} bytes",
                    path.display(),
                    previous_bytes,
                    bytes.len()
                )));
            }
        }

        let write_result = crate::engine::fs_utils::write_file_atomically_and_sync_parent_budgeted(
            path,
            bytes,
            Some(self),
            category,
            kind,
        );
        let Err(write_err) = write_result else {
            return Ok(());
        };

        let unchanged = match previous.as_deref() {
            Some(previous) => fs::read(path)
                .map(|current| current == previous)
                .unwrap_or(false),
            None => matches!(
                fs::symlink_metadata(path),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound
            ),
        };
        if unchanged {
            return Err(write_err);
        }

        let rollback_result = match previous {
            Some(previous) => {
                crate::engine::fs_utils::write_file_atomically_and_sync_parent_budgeted(
                    path,
                    &previous,
                    Some(self),
                    category,
                    DiskReservationKind::Recovery,
                )
            }
            None => crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
                path,
                Some(self),
                category,
            ),
        };
        match rollback_result {
            Ok(()) => Err(write_err),
            Err(rollback_err) => Err(TsinkError::Other(format!(
                "atomic write to {} failed: {write_err}; rollback failed: {rollback_err}",
                path.display()
            ))),
        }
    }

    /// Appends bytes to a managed regular file with normal-growth admission.
    ///
    /// Appends are serialized by the coordinator and the file is opened internally, binding the
    /// charged path to the mutated entry. On a write or sync failure this method first attempts to
    /// restore the original length or remove a newly-created file. If rollback fails, surviving
    /// growth is conservatively charged and the managed tree is reconciled before return.
    pub fn append_file_and_sync_parent(
        self: &Arc<Self>,
        path: &Path,
        bytes: &[u8],
        category: DiskCategory,
    ) -> Result<()> {
        self.append_file_and_sync_parent_with_kind(
            path,
            bytes,
            category,
            DiskReservationKind::Growth,
        )
    }

    /// Appends a cleanup or acknowledgement record with recovery admission.
    ///
    /// Recovery admission may proceed while reconciled usage is already above the logical quota,
    /// but it still enforces the configured physical free-space floor. Callers must pair temporary
    /// recovery growth with bounded cleanup that can reclaim the superseded state.
    pub fn append_file_and_sync_parent_for_recovery(
        self: &Arc<Self>,
        path: &Path,
        bytes: &[u8],
        category: DiskCategory,
    ) -> Result<()> {
        self.append_file_and_sync_parent_with_kind(
            path,
            bytes,
            category,
            DiskReservationKind::Recovery,
        )
    }

    fn append_file_and_sync_parent_with_kind(
        self: &Arc<Self>,
        path: &Path,
        bytes: &[u8],
        category: DiskCategory,
        kind: DiskReservationKind,
    ) -> Result<()> {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        let parent = path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed append file has no parent directory: {}",
                path.display()
            ))
        })?;
        self.create_dir_all_and_sync_parents(parent)?;
        self.validate_managed_file_path(path)?;
        let existed = fs::symlink_metadata(path).is_ok();
        let initial_len = if existed {
            fs::metadata(path)?.len()
        } else {
            0
        };
        let requested = u64::try_from(bytes.len()).map_err(|_| {
            TsinkError::Other("append byte count exceeds the supported byte range".to_string())
        })?;
        let reservation = self.reserve(category, requested, kind)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?;
        self.validate_managed_file_path(path)?;
        let append_result = (|| -> Result<()> {
            file.write_all(bytes)?;
            file.flush()?;
            file.sync_all()?;
            crate::engine::fs_utils::sync_parent_dir(path)?;
            Ok(())
        })();

        match append_result {
            Ok(()) => {
                let measured_growth = file
                    .metadata()
                    .map(|metadata| metadata.len().saturating_sub(initial_len))
                    .unwrap_or(requested);
                let settlement_result = reservation.commit(requested.max(measured_growth), 0);
                if let Err(settlement_err) = settlement_result {
                    let rollback_result = if existed {
                        file.set_len(initial_len)
                            .and_then(|()| file.sync_all())
                            .map_err(TsinkError::from)
                    } else {
                        drop(file);
                        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(path)
                    };
                    let reconciliation_result = self.reconcile_when_idle().map(|_| ());
                    return match (rollback_result, reconciliation_result) {
                        (Ok(()), Ok(())) => Err(settlement_err),
                        (rollback, reconciliation) => {
                            let mut errors =
                                vec![format!("disk settlement failed: {settlement_err}")];
                            if let Err(err) = rollback {
                                errors.push(format!("rollback failed: {err}"));
                            }
                            if let Err(err) = reconciliation {
                                errors.push(format!("disk reconciliation failed: {err}"));
                            }
                            Err(TsinkError::Other(format!(
                                "budgeted append to {} failed: {}",
                                path.display(),
                                errors.join("; ")
                            )))
                        }
                    };
                }
                Ok(())
            }
            Err(append_err) => {
                let rollback_result = if existed {
                    file.set_len(initial_len)
                        .and_then(|()| file.sync_all())
                        .map_err(TsinkError::from)
                } else {
                    drop(file);
                    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(path)
                };
                if rollback_result.is_ok() {
                    return Err(append_err);
                }

                let measured_growth = fs::metadata(path)
                    .map(|metadata| metadata.len().saturating_sub(initial_len))
                    .unwrap_or(requested);
                let settlement_result = reservation.commit(requested.max(measured_growth), 0);
                let reconciliation_result = self.reconcile_when_idle().map(|_| ());
                let mut errors = vec![format!("append failed: {append_err}")];
                if let Err(err) = rollback_result {
                    errors.push(format!("rollback failed: {err}"));
                }
                if let Err(err) = settlement_result {
                    errors.push(format!("disk settlement failed: {err}"));
                }
                if let Err(err) = reconciliation_result {
                    errors.push(format!("disk reconciliation failed: {err}"));
                }
                Err(TsinkError::Other(format!(
                    "budgeted append to {} failed: {}",
                    path.display(),
                    errors.join("; ")
                )))
            }
        }
    }

    pub(crate) fn matches_root(&self, path: &Path) -> Result<bool> {
        Ok(resolve_path_allow_missing(path)? == self.root)
    }

    /// Atomically reserves a peak additional byte footprint.
    ///
    /// Dropping the returned value releases the reservation. After the filesystem mutation
    /// succeeds, call [`DiskReservation::commit`] with the final persistent growth and any bytes
    /// removed from the same category.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        category: DiskCategory,
        bytes: u64,
        kind: DiskReservationKind,
    ) -> Result<DiskReservation> {
        let mut state = self.state.lock();
        while state.reconciliation_waiters > 0 {
            self.reservations_released.wait(&mut state);
        }
        let accounted = state.accounted_bytes();
        let outstanding = state.reserved_bytes;

        if kind != DiskReservationKind::Recovery {
            let Some(max_bytes) = self.limits.max_bytes else {
                // No logical limit is active, but physical headroom still applies below.
                return self.reserve_after_logical_check(category, bytes, kind, state);
            };
            let usable_limit = match kind {
                DiskReservationKind::Growth => {
                    max_bytes - self.limits.maintenance_temp_reserve_bytes
                }
                DiskReservationKind::Maintenance => max_bytes,
                DiskReservationKind::Recovery => unreachable!(),
            };
            let available = usable_limit
                .saturating_sub(accounted)
                .saturating_sub(outstanding);
            if bytes > available {
                state.rejections_total = state.rejections_total.saturating_add(1);
                return Err(logical_quota_error(
                    kind,
                    usable_limit,
                    accounted,
                    outstanding,
                    bytes,
                ));
            }
        }

        self.reserve_after_logical_check(category, bytes, kind, state)
    }

    /// Runs one maintenance mutation under a complete operation-level peak reservation.
    ///
    /// The reservation is admitted before `operation` can mutate the filesystem and remains live
    /// until an exclusive full-tree reconciliation installs the observed final accounting. The
    /// closure must not use nested budget helpers that themselves wait for idle reconciliation;
    /// covered writes and removals must instead rely on this outer reservation. This is intended
    /// for compaction, where several immutable outputs plus a replacement marker form one
    /// indivisible capacity decision but failures can leave recovery-owned artifacts behind.
    pub(crate) fn with_reconciled_maintenance_reservation<T, F>(
        self: &Arc<Self>,
        category: DiskCategory,
        peak_bytes: u64,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.with_reconciled_reservation(
            category,
            peak_bytes,
            DiskReservationKind::Maintenance,
            operation,
        )
    }

    /// Runs crash-recovery cleanup under physical peak admission while bypassing logical quota.
    pub(crate) fn with_reconciled_recovery_reservation<T, F>(
        self: &Arc<Self>,
        category: DiskCategory,
        peak_bytes: u64,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.with_reconciled_reservation(
            category,
            peak_bytes,
            DiskReservationKind::Recovery,
            operation,
        )
    }

    fn with_reconciled_reservation<T, F>(
        self: &Arc<Self>,
        category: DiskCategory,
        peak_bytes: u64,
        kind: DiskReservationKind,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        let reservation = self.reserve(category, peak_bytes, kind)?;
        let guard = ReconciledOperationReservation {
            reservation: Some(reservation),
        };
        let operation_result = operation();
        let reconciliation_result = guard.finish();
        match (operation_result, reconciliation_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(err), Ok(())) => Err(err),
            (Ok(value), Err(err)) => {
                // The mutation's caller still needs its committed catalog/result diff. Failed
                // reconciliation has already installed the guard's conservative peak fallback;
                // surface the accounting debt without converting a committed operation to an
                // error that would discard that result permanently.
                tracing::warn!(
                    error = %err,
                    "committed disk operation retained its result after reconciliation failure"
                );
                Ok(value)
            }
            (Err(operation_err), Err(reconciliation_err)) => Err(TsinkError::Other(format!(
                "disk operation failed: {operation_err}; disk reconciliation failed: {reconciliation_err}"
            ))),
        }
    }

    fn reserve_after_logical_check(
        self: &Arc<Self>,
        category: DiskCategory,
        bytes: u64,
        kind: DiskReservationKind,
        mut state: parking_lot::MutexGuard<'_, DiskAccountingState>,
    ) -> Result<DiskReservation> {
        let outstanding = state.reserved_bytes;

        let physical_reserve = match kind {
            DiskReservationKind::Growth => {
                self.limits.filesystem_free_headroom_bytes
                    + self.limits.maintenance_temp_reserve_bytes
            }
            DiskReservationKind::Maintenance => self.limits.filesystem_free_headroom_bytes,
            DiskReservationKind::Recovery => self.limits.filesystem_free_headroom_bytes,
        };
        if bytes > 0 {
            let available = self
                .space_probe
                .available_space(&self.root)
                .map_err(|source| TsinkError::IoWithPath {
                    path: self.root.clone(),
                    source,
                })?;
            let available_for_request = available
                .saturating_sub(outstanding)
                .saturating_sub(physical_reserve);
            if bytes > available_for_request {
                state.rejections_total = state.rejections_total.saturating_add(1);
                return Err(TsinkError::InsufficientDiskSpace {
                    required: bytes,
                    available: available_for_request,
                });
            }
        }

        state.admit_reservation(bytes, kind.uses_maintenance_capacity())?;

        Ok(DiskReservation {
            budget: Arc::clone(self),
            category,
            kind,
            reserved_bytes: bytes,
            active: true,
        })
    }

    /// Re-scans the root without following symlinks and replaces reconciled committed totals.
    ///
    /// Unknown and ambiguous files are counted under [`DiskCategory::Unknown`] and are never
    /// deleted by reconciliation.
    pub fn reconcile(&self) -> Result<LocalDiskBudgetSnapshot> {
        self.reconcile_with_memory_limit(usize::MAX)
    }

    pub(crate) fn reconcile_with_memory_limit(
        &self,
        memory_limit_bytes: usize,
    ) -> Result<LocalDiskBudgetSnapshot> {
        let mut state = self.state.lock();
        if state.active_reservations > 0 {
            return Err(TsinkError::Other(format!(
                "cannot reconcile local disk usage while {} reservation(s) are active",
                state.active_reservations
            )));
        }
        let categories = scan_tree_with_memory_limit(&self.root, memory_limit_bytes)?;
        checked_category_total(&categories)?;
        state.committed_by_category = categories;
        state.reconciliations_total = state.reconciliations_total.saturating_add(1);
        drop(state);
        Ok(self.snapshot())
    }

    /// Waits for live reservations, prevents new reservations from starting, and reconciles.
    ///
    /// Cleanup paths use this after filesystem shrinkage. Waiting rather than subtracting a
    /// measured category total prevents an externally-added file from causing an undercount of
    /// unrelated surviving data.
    pub fn reconcile_when_idle(&self) -> Result<LocalDiskBudgetSnapshot> {
        self.reconcile_when_idle_with_memory_limit(usize::MAX)
    }

    pub(crate) fn reconcile_when_idle_with_memory_limit(
        &self,
        memory_limit_bytes: usize,
    ) -> Result<LocalDiskBudgetSnapshot> {
        let mut state = self.state.lock();
        state.reconciliation_waiters =
            state.reconciliation_waiters.checked_add(1).ok_or_else(|| {
                TsinkError::Other("local disk reconciliation waiter counter overflow".to_string())
            })?;
        while state.active_reservations > 0 {
            self.reservations_released.wait(&mut state);
        }

        let categories_result = scan_tree_with_memory_limit(&self.root, memory_limit_bytes);
        let categories_result = categories_result.and_then(|categories| {
            checked_category_total(&categories)?;
            Ok(categories)
        });
        if let Ok(categories) = categories_result.as_ref() {
            state.committed_by_category = categories.clone();
            state.reconciliations_total = state.reconciliations_total.saturating_add(1);
        }
        state.reconciliation_waiters = state
            .reconciliation_waiters
            .checked_sub(1)
            .expect("local disk reconciliation waiter counter underflow");
        drop(state);
        self.reservations_released.notify_all();
        categories_result?;
        Ok(self.snapshot())
    }

    /// Returns current committed usage, reservations, counters, and best-effort free space.
    pub fn snapshot(&self) -> LocalDiskBudgetSnapshot {
        let filesystem_available_bytes = self.space_probe.available_space(&self.root).ok();
        let state = self.state.lock();
        let accounted_bytes = state.accounted_bytes();
        let categories = state
            .committed_by_category
            .iter()
            .filter_map(|(category, bytes)| {
                (*bytes > 0).then_some(DiskCategoryUsage {
                    category: *category,
                    bytes: *bytes,
                })
            })
            .collect();
        LocalDiskBudgetSnapshot {
            limits: self.limits,
            accounted_bytes,
            reserved_bytes: state.reserved_bytes,
            maintenance_reserved_bytes: state.maintenance_reserved_bytes,
            unknown_bytes: state.category_bytes(DiskCategory::Unknown),
            filesystem_available_bytes,
            over_limit: self
                .limits
                .max_bytes
                .is_some_and(|max_bytes| accounted_bytes > max_bytes),
            active_reservations: state.active_reservations,
            rejections_total: state.rejections_total,
            reconciliations_total: state.reconciliations_total,
            reservation_overruns_total: state.reservation_overruns_total,
            categories,
        }
    }

    pub(crate) fn status_snapshot_modeled_retained_bytes(&self) -> Result<u64> {
        crate::storage::modeled_status_observability_vec_bytes::<DiskCategoryUsage>(
            DISK_CATEGORY_COUNT,
        )
    }

    /// Materializes the complete local-disk status shape after its fixed category capacity has
    /// already been reserved on the caller's query execution.
    pub(crate) fn status_snapshot_after_reservation(
        &self,
        execution: &crate::QueryExecution,
    ) -> Result<LocalDiskBudgetSnapshot> {
        execution.checkpoint()?;
        let filesystem_available_bytes = self.space_probe.available_space(&self.root).ok();
        execution.checkpoint()?;
        let state = self.state.lock();
        execution.checkpoint()?;
        let accounted_bytes = state.accounted_bytes();
        let mut categories = Vec::new();
        categories
            .try_reserve_exact(DISK_CATEGORY_COUNT)
            .map_err(|_| {
                TsinkError::Other(
                    "storage status local-disk category allocation failed".to_string(),
                )
            })?;
        for category in DISK_CATEGORIES {
            let bytes = state.category_bytes(category);
            if bytes > 0 {
                categories.push(DiskCategoryUsage { category, bytes });
            }
        }
        Ok(LocalDiskBudgetSnapshot {
            limits: self.limits,
            accounted_bytes,
            reserved_bytes: state.reserved_bytes,
            maintenance_reserved_bytes: state.maintenance_reserved_bytes,
            unknown_bytes: state.category_bytes(DiskCategory::Unknown),
            filesystem_available_bytes,
            over_limit: self
                .limits
                .max_bytes
                .is_some_and(|max_bytes| accounted_bytes > max_bytes),
            active_reservations: state.active_reservations,
            rejections_total: state.rejections_total,
            reconciliations_total: state.reconciliations_total,
            reservation_overruns_total: state.reservation_overruns_total,
            categories,
        })
    }

    /// Returns the allocation-free metrics projection under an existing query execution.
    ///
    /// The fixed category array is populated directly while the producer lock is held. Call
    /// [`LocalDiskMetricsSnapshot::categories`] to iterate its non-empty entries without
    /// reconstructing an owned collection.
    pub fn metrics_snapshot_with_execution(
        &self,
        execution: &crate::QueryExecution,
    ) -> Result<LocalDiskMetricsSnapshot> {
        execution.checkpoint()?;
        let filesystem_available_bytes = self.space_probe.available_space(&self.root).ok();
        execution.checkpoint()?;
        let state = self.state.lock();
        execution.checkpoint()?;
        let accounted_bytes = state.accounted_bytes();
        let category_bytes = DISK_CATEGORIES.map(|category| state.category_bytes(category));
        Ok(LocalDiskMetricsSnapshot {
            limits: self.limits,
            accounted_bytes,
            reserved_bytes: state.reserved_bytes,
            maintenance_reserved_bytes: state.maintenance_reserved_bytes,
            unknown_bytes: state.category_bytes(DiskCategory::Unknown),
            filesystem_available_bytes,
            over_limit: self
                .limits
                .max_bytes
                .is_some_and(|max_bytes| accounted_bytes > max_bytes),
            active_reservations: state.active_reservations,
            rejections_total: state.rejections_total,
            reconciliations_total: state.reconciliations_total,
            reservation_overruns_total: state.reservation_overruns_total,
            category_bytes,
        })
    }

    fn finish_reservation(
        &self,
        category: DiskCategory,
        kind: DiskReservationKind,
        reserved_bytes: u64,
        added_bytes: u64,
        removed_bytes: u64,
    ) -> Result<()> {
        let mut state = self.state.lock();
        let became_idle =
            state.release_reservation(reserved_bytes, kind.uses_maintenance_capacity());
        if added_bytes > reserved_bytes {
            state.reservation_overruns_total = state.reservation_overruns_total.saturating_add(1);
        }
        let accounting_result = state.apply_committed_delta(category, added_bytes, removed_bytes);
        drop(state);
        if became_idle {
            self.reservations_released.notify_all();
        }
        let overrun_result = if added_bytes > reserved_bytes {
            Err(TsinkError::InsufficientDiskSpace {
                required: added_bytes,
                available: reserved_bytes,
            })
        } else {
            Ok(())
        };
        match (accounting_result, overrun_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
            (Err(accounting_err), Err(overrun_err)) => Err(TsinkError::Other(format!(
                "local disk reservation overrun and accounting settlement failed: {overrun_err}; {accounting_err}"
            ))),
        }
    }

    fn finish_reservation_by_reconciling(
        &self,
        category: DiskCategory,
        kind: DiskReservationKind,
        reserved_bytes: u64,
    ) -> Result<()> {
        let mut state = self.state.lock();
        state.reconciliation_waiters =
            state.reconciliation_waiters.checked_add(1).ok_or_else(|| {
                TsinkError::Other("local disk reconciliation waiter counter overflow".to_string())
            })?;

        state.release_reservation(reserved_bytes, kind.uses_maintenance_capacity());
        while state.active_reservations > 0 {
            self.reservations_released.wait(&mut state);
        }

        let categories_result = scan_tree(&self.root).and_then(|categories| {
            checked_category_total(&categories)?;
            Ok(categories)
        });
        let fallback_accounting_result = match categories_result.as_ref() {
            Ok(categories) => {
                state.committed_by_category = categories.clone();
                state.reconciliations_total = state.reconciliations_total.saturating_add(1);
                Ok(())
            }
            Err(_) => {
                // Keep failed exact scans conservative: the operation could have published up to
                // the admitted staging peak, while removals only make the stale pre-operation
                // total an overcount. A later successful reconciliation removes this fallback.
                state.apply_committed_delta(category, reserved_bytes, 0)
            }
        };
        state.reconciliation_waiters = state
            .reconciliation_waiters
            .checked_sub(1)
            .expect("local disk reconciliation waiter counter underflow");
        drop(state);
        self.reservations_released.notify_all();
        match (categories_result.map(|_| ()), fallback_accounting_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
            (Err(scan_err), Err(accounting_err)) => Err(TsinkError::Other(format!(
                "local disk reconciliation failed: {scan_err}; conservative fallback accounting failed: {accounting_err}"
            ))),
        }
    }

    fn cancel_reservation(&self, kind: DiskReservationKind, reserved_bytes: u64) {
        let mut state = self.state.lock();
        let became_idle =
            state.release_reservation(reserved_bytes, kind.uses_maintenance_capacity());
        drop(state);
        if became_idle {
            self.reservations_released.notify_all();
        }
    }

    fn grow_reservation(&self, kind: DiskReservationKind, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut state = self.state.lock();
        let accounted = state.accounted_bytes();
        let outstanding = state.reserved_bytes;
        if kind != DiskReservationKind::Recovery {
            let Some(max_bytes) = self.limits.max_bytes else {
                return self.grow_reservation_after_logical_check(kind, bytes, state);
            };
            let usable_limit = match kind {
                DiskReservationKind::Growth => {
                    max_bytes - self.limits.maintenance_temp_reserve_bytes
                }
                DiskReservationKind::Maintenance => max_bytes,
                DiskReservationKind::Recovery => unreachable!(),
            };
            let available = usable_limit
                .saturating_sub(accounted)
                .saturating_sub(outstanding);
            if bytes > available {
                state.rejections_total = state.rejections_total.saturating_add(1);
                return Err(logical_quota_error(
                    kind,
                    usable_limit,
                    accounted,
                    outstanding,
                    bytes,
                ));
            }
        }

        self.grow_reservation_after_logical_check(kind, bytes, state)
    }

    fn grow_reservation_after_logical_check(
        &self,
        kind: DiskReservationKind,
        bytes: u64,
        mut state: parking_lot::MutexGuard<'_, DiskAccountingState>,
    ) -> Result<()> {
        let outstanding = state.reserved_bytes;

        let physical_reserve = match kind {
            DiskReservationKind::Growth => {
                self.limits.filesystem_free_headroom_bytes
                    + self.limits.maintenance_temp_reserve_bytes
            }
            DiskReservationKind::Maintenance => self.limits.filesystem_free_headroom_bytes,
            DiskReservationKind::Recovery => self.limits.filesystem_free_headroom_bytes,
        };
        let available = self
            .space_probe
            .available_space(&self.root)
            .map_err(|source| TsinkError::IoWithPath {
                path: self.root.clone(),
                source,
            })?;
        let available_for_request = available
            .saturating_sub(outstanding)
            .saturating_sub(physical_reserve);
        if bytes > available_for_request {
            state.rejections_total = state.rejections_total.saturating_add(1);
            return Err(TsinkError::InsufficientDiskSpace {
                required: bytes,
                available: available_for_request,
            });
        }

        state.grow_reservation(bytes, kind.uses_maintenance_capacity())
    }
}

/// RAII ownership of bytes admitted by a [`LocalDiskBudget`].
#[must_use = "dropping a disk reservation releases it without committing usage"]
pub(crate) struct DiskReservation {
    budget: Arc<LocalDiskBudget>,
    category: DiskCategory,
    kind: DiskReservationKind,
    reserved_bytes: u64,
    active: bool,
}

/// Unwind guard for a mutation covered by one aggregate operation reservation.
///
/// Ordinary completion replaces the reservation with an exact tree scan. If the closure unwinds,
/// `Drop` cannot safely perform fallible filesystem work, so it commits the complete admitted peak
/// as conservative logical usage. A later successful reconciliation removes any overcharge.
struct ReconciledOperationReservation {
    reservation: Option<DiskReservation>,
}

impl ReconciledOperationReservation {
    fn finish(mut self) -> Result<()> {
        self.reservation
            .take()
            .expect("operation reconciliation guard must own a reservation")
            .finish_by_reconciling()
    }
}

impl Drop for ReconciledOperationReservation {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            let peak_bytes = reservation.reserved_bytes();
            let _ = reservation.commit(peak_bytes, 0);
        }
    }
}

impl fmt::Debug for DiskReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiskReservation")
            .field("category", &self.category)
            .field("kind", &self.kind)
            .field("reserved_bytes", &self.reserved_bytes)
            .field("active", &self.active)
            .finish()
    }
}

impl DiskReservation {
    /// Atomically expands this reservation before additional filesystem growth.
    pub(crate) fn grow_by(&mut self, bytes: u64) -> Result<()> {
        let reserved_bytes = self.reserved_bytes.checked_add(bytes).ok_or_else(|| {
            TsinkError::Other(format!(
                "local disk reservation handle overflow: current={}, added={bytes}",
                self.reserved_bytes
            ))
        })?;
        self.budget.grow_reservation(self.kind, bytes)?;
        self.reserved_bytes = reserved_bytes;
        Ok(())
    }

    /// Converts the reservation into a committed accounting delta.
    ///
    /// `added_bytes` is the final persistent growth, not the temporary peak. `removed_bytes` is
    /// space removed from the same category by the operation. The reservation must have covered
    /// the operation's peak additional footprint even when the final delta is smaller.
    pub(crate) fn commit(mut self, added_bytes: u64, removed_bytes: u64) -> Result<()> {
        let result = self.budget.finish_reservation(
            self.category,
            self.kind,
            self.reserved_bytes,
            added_bytes,
            removed_bytes,
        );
        self.active = false;
        result
    }

    pub(crate) fn commit_as(
        mut self,
        category: DiskCategory,
        added_bytes: u64,
        removed_bytes: u64,
    ) -> Result<()> {
        let result = self.budget.finish_reservation(
            category,
            self.kind,
            self.reserved_bytes,
            added_bytes,
            removed_bytes,
        );
        self.active = false;
        result
    }

    /// Releases this peak reservation by atomically installing an exact full-tree scan.
    ///
    /// The budget blocks new admission before releasing the reservation and waits for every other
    /// live reservation before scanning, so no caller can observe a stale committed total between
    /// release and reconciliation.
    fn finish_by_reconciling(mut self) -> Result<()> {
        let result = self.budget.finish_reservation_by_reconciling(
            self.category,
            self.kind,
            self.reserved_bytes,
        );
        self.active = false;
        result
    }

    /// Returns the admitted peak byte count.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }
}

fn logical_quota_error(
    kind: DiskReservationKind,
    limit: u64,
    used: u64,
    reserved: u64,
    requested: u64,
) -> TsinkError {
    match kind {
        DiskReservationKind::Growth => TsinkError::DiskQuotaExceeded {
            limit,
            used,
            reserved,
            requested,
        },
        DiskReservationKind::Maintenance => TsinkError::InsufficientCompactionHeadroom {
            limit,
            used,
            reserved,
            requested,
        },
        DiskReservationKind::Recovery => unreachable!("recovery bypasses the logical quota"),
    }
}

fn absolute_path_lexically_normalized(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(TsinkError::Io)?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
        }
    }
    Ok(normalized)
}

fn resolve_path_allow_missing(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(TsinkError::Io)?.join(path)
    };
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                resolved.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
                if let Ok(canonical) = fs::canonicalize(&resolved) {
                    resolved = canonical;
                }
            }
            Component::Normal(name) => {
                resolved.push(name);
                match fs::canonicalize(&resolved) {
                    Ok(canonical) => resolved = canonical,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(TsinkError::IoWithPath {
                            path: resolved,
                            source,
                        });
                    }
                }
            }
        }
    }
    Ok(resolved)
}

fn resolve_entry_path_no_follow(path: &Path) -> Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "managed file has no final component: {}",
            path.display()
        ))
    })?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    Ok(resolve_path_allow_missing(parent)?.join(file_name))
}

fn validate_regular_file_entry(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(metadata) => Err(TsinkError::InvalidConfiguration(format!(
            "staged replacement target must be a regular file, found {:?}: {}",
            metadata.file_type(),
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn sync_directory_ancestry(directory: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in directory.components() {
        current.push(component.as_os_str());
        if !matches!(component, Component::Normal(_)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&current).map_err(|source| TsinkError::IoWithPath {
            path: current.clone(),
            source,
        })?;
        if !metadata.file_type().is_dir() {
            return Err(TsinkError::InvalidConfiguration(format!(
                "managed directory path contains a non-directory entry: {}",
                current.display()
            )));
        }
        crate::engine::fs_utils::sync_parent_dir(&current)?;
    }
    Ok(())
}

pub(crate) fn measured_path_bytes(path: &Path) -> Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if metadata.is_dir() && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) {
        let categories = scan_tree(path)?;
        return checked_category_total(&categories);
    }
    if metadata.is_file() || crate::engine::fs_utils::is_link_or_reparse_point(&metadata) {
        return Ok(metadata.len());
    }
    Ok(0)
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        if self.active {
            self.budget
                .cancel_reservation(self.kind, self.reserved_bytes);
        }
    }
}

fn scan_tree(root: &Path) -> Result<BTreeMap<DiskCategory, u64>> {
    scan_tree_with_memory_limit(root, usize::MAX)
}

fn scan_tree_with_memory_limit(
    root: &Path,
    memory_limit_bytes: usize,
) -> Result<BTreeMap<DiskCategory, u64>> {
    scan_path_with_memory_limit(root, root, memory_limit_bytes)
}

fn scan_path_with_memory_limit(
    root: &Path,
    path: &Path,
    memory_limit_bytes: usize,
) -> Result<BTreeMap<DiskCategory, u64>> {
    scan_path_with_namespace_limits(
        root,
        path,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
        memory_limit_bytes,
    )
}

fn scan_path_with_namespace_limits(
    root: &Path,
    path: &Path,
    max_entries: usize,
    max_depth: u32,
    memory_limit_bytes: usize,
) -> Result<BTreeMap<DiskCategory, u64>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if metadata.is_file() || crate::engine::fs_utils::is_link_or_reparse_point(&metadata) {
        admit_startup_memory(memory_limit_bytes, DISK_RECONCILIATION_FIXED_BYTES)?;
        let mut categories = BTreeMap::new();
        categories.insert(classify_path(root, path), metadata.len());
        return Ok(categories);
    }
    if !metadata.is_dir() {
        return Ok(BTreeMap::new());
    }

    admit_startup_memory(memory_limit_bytes, DISK_RECONCILIATION_FIXED_BYTES)?;
    let mut totals = [0u64; DISK_CATEGORY_COUNT];
    let mut observed_entries = 0usize;
    scan_directory_streaming(
        root,
        path,
        0,
        0,
        max_entries,
        max_depth,
        memory_limit_bytes,
        &mut observed_entries,
        &mut totals,
    )?;
    let mut categories = BTreeMap::new();
    for (category, total) in DISK_CATEGORIES.into_iter().zip(totals) {
        if total > 0 {
            categories.insert(category, total);
        }
    }
    Ok(categories)
}

const DISK_CATEGORIES: [DiskCategory; 12] = [
    DiskCategory::Wal,
    DiskCategory::Segments,
    DiskCategory::Registry,
    DiskCategory::Tombstones,
    DiskCategory::Rollups,
    DiskCategory::Metadata,
    DiskCategory::Exemplars,
    DiskCategory::Cluster,
    DiskCategory::EdgeSync,
    DiskCategory::ServerState,
    DiskCategory::Temporary,
    DiskCategory::Unknown,
];
const DISK_CATEGORY_COUNT: usize = DISK_CATEGORIES.len();
// The fixed array is the only category state retained while walking. The second term admits a
// conservative node/header allowance for the at-most-twelve BTreeMap entries materialized after
// the walk; it is independent of namespace cardinality.
const DISK_RECONCILIATION_FIXED_BYTES: usize = std::mem::size_of::<[u64; DISK_CATEGORY_COUNT]>()
    + std::mem::size_of::<BTreeMap<DiskCategory, u64>>()
    + std::mem::size_of::<fs::ReadDir>()
    + DISK_CATEGORY_COUNT
        * (std::mem::size_of::<(DiskCategory, u64)>() + 4 * std::mem::size_of::<usize>());

#[allow(clippy::too_many_arguments)]
fn scan_directory_streaming(
    root: &Path,
    directory: &Path,
    depth: u32,
    retained_path_bytes: usize,
    max_entries: usize,
    max_depth: u32,
    memory_limit_bytes: usize,
    observed_entries: &mut usize,
    totals: &mut [u64; DISK_CATEGORY_COUNT],
) -> Result<()> {
    let active_depth = usize::try_from(depth)
        .unwrap_or(usize::MAX)
        .checked_add(1)
        .ok_or_else(|| {
            TsinkError::Other("local disk reconciliation depth size overflow".to_string())
        })?;
    let frame_bytes = active_depth
        .checked_mul(
            std::mem::size_of::<fs::ReadDir>()
                + std::mem::size_of::<fs::DirEntry>()
                + std::mem::size_of::<PathBuf>(),
        )
        .ok_or_else(|| {
            TsinkError::Other("local disk reconciliation frame size overflow".to_string())
        })?;
    let directory_required = DISK_RECONCILIATION_FIXED_BYTES
        .checked_add(retained_path_bytes)
        .and_then(|bytes| bytes.checked_add(frame_bytes))
        .ok_or_else(|| {
            TsinkError::Other("local disk reconciliation memory model overflow".to_string())
        })?;
    admit_startup_memory(memory_limit_bytes, directory_required)?;
    let read_dir = fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in read_dir {
        if *observed_entries == max_entries {
            return Err(TsinkError::DataCorruption(format!(
                "local disk exact reconciliation exceeds its {max_entries}-entry global work bound: {}",
                directory.display()
            )));
        }
        *observed_entries = observed_entries.checked_add(1).ok_or_else(|| {
            TsinkError::Other(format!(
                "local disk reconciliation namespace entry counter overflow at {}",
                directory.display()
            ))
        })?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        let file_name = entry.file_name();
        let component_bytes = file_name.as_encoded_bytes().len();
        let path_bytes = directory
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(component_bytes)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| {
                TsinkError::Other(format!(
                    "local disk reconciliation path size overflow at {}",
                    directory.display()
                ))
            })?;
        let required = DISK_RECONCILIATION_FIXED_BYTES
            .checked_add(retained_path_bytes)
            .and_then(|bytes| bytes.checked_add(frame_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
            .and_then(|bytes| bytes.checked_add(component_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
            .and_then(|bytes| bytes.checked_add(path_bytes))
            .ok_or_else(|| {
                TsinkError::Other("local disk reconciliation memory model overflow".to_string())
            })?;
        admit_startup_memory(memory_limit_bytes, required)?;

        let path = directory.join(&file_name);
        let metadata = fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
        let file_type = metadata.file_type();
        if file_type.is_dir() && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) {
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                TsinkError::Other(format!(
                    "local disk reconciliation depth counter overflow at {}",
                    path.display()
                ))
            })?;
            if child_depth > max_depth {
                return Err(TsinkError::DataCorruption(format!(
                    "local disk exact reconciliation exceeds its {max_depth}-level recursive depth bound: {}",
                    path.display()
                )));
            }
            let child_retained = retained_path_bytes
                .checked_add(std::mem::size_of::<PathBuf>())
                .and_then(|bytes| bytes.checked_add(path.capacity()))
                .ok_or_else(|| {
                    TsinkError::Other(
                        "local disk reconciliation retained-path size overflow".to_string(),
                    )
                })?;
            scan_directory_streaming(
                root,
                &path,
                child_depth,
                child_retained,
                max_entries,
                max_depth,
                memory_limit_bytes,
                observed_entries,
                totals,
            )?;
        } else if file_type.is_file()
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        {
            let category = classify_path(root, &path);
            let total = &mut totals[disk_category_index(category)];
            *total = total.checked_add(metadata.len()).ok_or_else(|| {
                TsinkError::Other(format!(
                    "local disk accounting overflow while scanning category {category:?} at {}",
                    path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn disk_category_index(category: DiskCategory) -> usize {
    DISK_CATEGORIES
        .iter()
        .position(|candidate| *candidate == category)
        .expect("every disk category must have a fixed reconciliation slot")
}

pub(crate) fn admit_startup_memory(memory_limit_bytes: usize, required: usize) -> Result<()> {
    if memory_limit_bytes != usize::MAX && required > memory_limit_bytes {
        return Err(TsinkError::MemoryBudgetExceeded {
            budget: memory_limit_bytes,
            required,
        });
    }
    Ok(())
}

fn checked_category_total(categories: &BTreeMap<DiskCategory, u64>) -> Result<u64> {
    categories.values().try_fold(0u64, |total, bytes| {
        total.checked_add(*bytes).ok_or_else(|| {
            TsinkError::Other(
                "local disk accounting total exceeds the supported byte range".to_string(),
            )
        })
    })
}

fn modeled_path_vectors_bytes(left: &Vec<PathBuf>, right: &Vec<PathBuf>) -> Result<usize> {
    let vector_storage = left
        .capacity()
        .checked_add(right.capacity())
        .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<PathBuf>()))
        .ok_or_else(|| {
            TsinkError::Other("startup cleanup path-vector capacity overflow".to_string())
        })?;
    left.iter().chain(right).try_fold(
        2usize
            .checked_mul(std::mem::size_of::<Vec<PathBuf>>())
            .and_then(|bytes| bytes.checked_add(vector_storage))
            .ok_or_else(|| {
                TsinkError::Other("startup cleanup memory model overflow".to_string())
            })?,
        |total, path| {
            total.checked_add(path.capacity()).ok_or_else(|| {
                TsinkError::Other("startup cleanup retained-path overflow".to_string())
            })
        },
    )
}

fn push_modeled_cleanup_path(
    target: &mut Vec<PathBuf>,
    path: PathBuf,
    other: &Vec<PathBuf>,
    memory_limit_bytes: usize,
    operation: &str,
) -> Result<()> {
    let prospective_capacity = if target.len() == target.capacity() {
        target
            .len()
            .checked_add(1)
            .ok_or_else(|| TsinkError::Other(format!("{operation} path-vector length overflow")))?
    } else {
        target.capacity()
    };
    let prospective_storage = prospective_capacity
        .checked_add(other.capacity())
        .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<PathBuf>()))
        .ok_or_else(|| TsinkError::Other(format!("{operation} path capacity overflow")))?;
    let prospective_paths =
        target
            .iter()
            .chain(other)
            .try_fold(path.capacity(), |total, retained| {
                total
                    .checked_add(retained.capacity())
                    .ok_or_else(|| TsinkError::Other(format!("{operation} retained-path overflow")))
            })?;
    let prospective = 2usize
        .checked_mul(std::mem::size_of::<Vec<PathBuf>>())
        .and_then(|bytes| bytes.checked_add(prospective_storage))
        .and_then(|bytes| bytes.checked_add(prospective_paths))
        .ok_or_else(|| TsinkError::Other(format!("{operation} memory model overflow")))?;
    admit_startup_memory(memory_limit_bytes, prospective)?;
    if target.len() == target.capacity() {
        target.try_reserve_exact(1).map_err(|_| {
            TsinkError::Other(format!(
                "unable to allocate bounded path plan for {operation}"
            ))
        })?;
    }
    target.push(path);
    admit_startup_memory(
        memory_limit_bytes,
        modeled_path_vectors_bytes(target, other)?,
    )
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
    if !canonical_pid || !is_exact_lower_hex(nonce, 16) {
        return None;
    }
    Some(target)
}

fn is_exact_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn classify_path(root: &Path, path: &Path) -> DiskCategory {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let file_name = relative
        .file_name()
        .and_then(|component| component.to_str())
        .unwrap_or_default();

    if path_component_strs(relative).any(|component| {
        component.eq_ignore_ascii_case(".compaction-replacements")
            || component.eq_ignore_ascii_case(".post-flush-replacements")
            || ascii_starts_with_ignore_case(component, ".tmp-tsink-")
            || ascii_starts_with_ignore_case(component, ".tmp-seg-")
            || ascii_starts_with_ignore_case(component, ".tsink-post-flush-retired-")
            || ascii_starts_with_ignore_case(component, ".tsink-post-flush-rollback-")
            || (component.starts_with('.') && ascii_contains_ignore_case(component, ".tmp-"))
    }) || file_name.eq_ignore_ascii_case("wal.published.tmp")
    {
        return DiskCategory::Temporary;
    }
    if path_component_strs(relative)
        .next()
        .is_some_and(|component| component.eq_ignore_ascii_case("wal"))
    {
        return DiskCategory::Wal;
    }
    if file_name.eq_ignore_ascii_case("tombstones.json")
        || path_component_strs(relative)
            .any(|component| component.eq_ignore_ascii_case("tombstones.json.store"))
    {
        return DiskCategory::Tombstones;
    }
    if file_name.eq_ignore_ascii_case("series_index.bin")
        || file_name.eq_ignore_ascii_case("series_index.delta.bin")
        || file_name.eq_ignore_ascii_case("series_index.catalog.json")
        || file_name.eq_ignore_ascii_case("segment_catalog.json")
        || file_name.eq_ignore_ascii_case(
            crate::engine::storage_engine::data_directory_manifest::
                DATA_DIRECTORY_MANIFEST_FILE_NAME,
        )
        || path_component_strs(relative)
            .any(|component| component.eq_ignore_ascii_case("series_index.delta.d"))
    {
        return DiskCategory::Registry;
    }
    {
        let mut components = path_component_strs(relative);
        let first = components.next();
        let second = components.next();
        let third = components.next();
        let fourth = components.next();
        let has_file_below_segment_root = components.next().is_some();
        if first.is_some_and(|component| {
            component.eq_ignore_ascii_case("lane_numeric")
                || component.eq_ignore_ascii_case("lane_blob")
        }) && second.is_some_and(|component| component.eq_ignore_ascii_case("segments"))
            && third
                .and_then(|component| strip_ascii_prefix_ignore_case(component, "l"))
                .is_some_and(|level| {
                    !level.is_empty() && level.bytes().all(|byte| byte.is_ascii_digit())
                })
            && fourth
                .and_then(|component| strip_ascii_prefix_ignore_case(component, "seg-"))
                .is_some_and(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
            && has_file_below_segment_root
        {
            return DiskCategory::Segments;
        }
    }
    if path_component_strs(relative)
        .next()
        .is_some_and(|component| component.eq_ignore_ascii_case(".rollups"))
    {
        return DiskCategory::Rollups;
    }
    if path_component_strs(relative)
        .next()
        .is_some_and(|component| component.eq_ignore_ascii_case("edge_sync"))
    {
        return DiskCategory::EdgeSync;
    }
    if path_component_strs(relative).any(|component| {
        ascii_contains_ignore_case(component, "cluster")
            || ascii_contains_ignore_case(component, "outbox")
            || ascii_contains_ignore_case(component, "dedupe")
            || ascii_contains_ignore_case(component, "audit")
            || ascii_contains_ignore_case(component, "consensus")
    }) {
        return DiskCategory::Cluster;
    }
    if file_name.eq_ignore_ascii_case("metric-metadata-store.json") {
        return DiskCategory::Metadata;
    }
    if file_name.eq_ignore_ascii_case("exemplar-store.json") {
        return DiskCategory::Exemplars;
    }
    if file_name.eq_ignore_ascii_case("rules-store.json")
        || path_component_strs(relative).any(|component| {
            component.eq_ignore_ascii_case("usage-accounting")
                || component.eq_ignore_ascii_case("managed-control-plane")
        })
    {
        return DiskCategory::ServerState;
    }
    DiskCategory::Unknown
}

fn path_component_strs(path: &Path) -> impl Iterator<Item = &str> {
    path.components()
        .filter_map(|component| component.as_os_str().to_str())
}

fn ascii_starts_with_ignore_case(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn strip_ascii_prefix_ignore_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    ascii_starts_with_ignore_case(value, prefix).then(|| &value[prefix.len()..])
}

fn ascii_contains_ignore_case(value: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    value
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

#[cfg(unix)]
fn system_statvfs(path: &Path) -> std::io::Result<libc::statvfs> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "disk-budget path contains an interior NUL byte",
        )
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` points to writable storage for statvfs.
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: statvfs returned success and initialized the output structure.
    Ok(unsafe { stats.assume_init() })
}

#[cfg(unix)]
fn system_allocation_unit(path: &Path) -> std::io::Result<u64> {
    let stats = system_statvfs(path)?;
    let fragment_size = if stats.f_frsize == 0 {
        stats.f_bsize
    } else {
        stats.f_frsize
    };
    let fragment_size = fragment_size as u64;
    if fragment_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "filesystem reported a zero-byte allocation unit",
        ));
    }
    Ok(fragment_size)
}

#[cfg(unix)]
fn system_available_space(path: &Path) -> std::io::Result<u64> {
    let stats = system_statvfs(path)?;
    let fragment_size = system_allocation_unit(path)?;
    (stats.f_bavail as u64)
        .checked_mul(fragment_size)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "filesystem available-space byte count overflowed",
            )
        })
}

#[cfg(windows)]
fn system_allocation_unit(path: &Path) -> std::io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceW(
            root_path_name: *const u16,
            sectors_per_cluster: *mut u32,
            bytes_per_sector: *mut u32,
            number_of_free_clusters: *mut u32,
            total_number_of_clusters: *mut u32,
        ) -> i32;
    }

    let volume_root = path.ancestors().last().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("filesystem path has no volume root: {}", path.display()),
        )
    })?;
    let mut volume_root: Vec<u16> = volume_root.as_os_str().encode_wide().collect();
    volume_root.push(0);
    let mut sectors_per_cluster = 0u32;
    let mut bytes_per_sector = 0u32;
    let result = unsafe {
        GetDiskFreeSpaceW(
            volume_root.as_ptr(),
            &mut sectors_per_cluster,
            &mut bytes_per_sector,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if result == 0 {
        return Err(std::io::Error::last_os_error());
    }
    u64::from(sectors_per_cluster)
        .checked_mul(u64::from(bytes_per_sector))
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "filesystem allocation-unit byte count was zero or overflowed",
            )
        })
}

#[cfg(windows)]
fn system_available_space(path: &Path) -> std::io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    use std::ptr;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            directory_name: *const u16,
            free_bytes_available: *mut u64,
            total_number_of_bytes: *mut u64,
            total_number_of_free_bytes: *mut u64,
        ) -> i32;
    }

    let mut path: Vec<u16> = path.as_os_str().encode_wide().collect();
    path.push(0);
    let mut available = 0u64;
    // SAFETY: `path` is NUL-terminated, `available` is writable, and unused outputs are null.
    let result = unsafe {
        GetDiskFreeSpaceExW(
            path.as_ptr(),
            &mut available,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    if result == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(available)
}

#[cfg(not(any(unix, windows)))]
fn system_available_space(_path: &Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "filesystem free-space probing is unsupported on this target",
    ))
}

#[cfg(not(any(unix, windows)))]
fn system_allocation_unit(_path: &Path) -> std::io::Result<u64> {
    Ok(crate::SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct FixedSpaceProbe(AtomicU64);

    impl FixedSpaceProbe {
        fn new(bytes: u64) -> Self {
            Self(AtomicU64::new(bytes))
        }
    }

    impl SpaceProbe for FixedSpaceProbe {
        fn available_space(&self, _path: &Path) -> std::io::Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    fn budget_with_space(
        root: &Path,
        limits: LocalDiskLimits,
        available: u64,
    ) -> Arc<LocalDiskBudget> {
        LocalDiskBudget::open_with_space_probe(
            root,
            limits,
            Arc::new(FixedSpaceProbe::new(available)),
        )
        .unwrap()
    }

    #[test]
    fn metrics_snapshot_iterates_fixed_categories_in_stable_order() {
        let dir = TempDir::new().unwrap();
        let budget = budget_with_space(dir.path(), LocalDiskLimits::default(), 1_000_000);
        {
            let mut state = budget.state.lock();
            state.committed_by_category.insert(DiskCategory::Unknown, 7);
            state.committed_by_category.insert(DiskCategory::Rollups, 5);
            state.committed_by_category.insert(DiskCategory::Wal, 3);
        }
        let query_budget = crate::QueryBudget::new(crate::QueryBudgetLimits::default()).unwrap();
        let execution = query_budget.begin_query().unwrap();
        let snapshot = budget.metrics_snapshot_with_execution(&execution).unwrap();
        assert_eq!(
            snapshot.categories().collect::<Vec<_>>(),
            vec![
                DiskCategoryUsage {
                    category: DiskCategory::Wal,
                    bytes: 3,
                },
                DiskCategoryUsage {
                    category: DiskCategory::Rollups,
                    bytes: 5,
                },
                DiskCategoryUsage {
                    category: DiskCategory::Unknown,
                    bytes: 7,
                },
            ]
        );
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    }

    #[test]
    fn limits_reject_unrepresentable_combined_physical_reserve() {
        let err = LocalDiskLimits {
            filesystem_free_headroom_bytes: u64::MAX,
            maintenance_temp_reserve_bytes: 1,
            ..LocalDiskLimits::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
        assert!(err.to_string().contains("exceeds the supported byte range"));
    }

    #[test]
    fn directory_replacement_reports_committed_reconciliation_failure_explicitly() {
        let err = combine_managed_directory_replacement::<()>(
            Ok(()),
            Err(TsinkError::Other("injected scan failure".to_string())),
        )
        .unwrap_err();

        assert!(err.to_string().contains(
            "managed directory replacement committed but accounting reconciliation failed"
        ));
        assert!(err.to_string().contains("injected scan failure"));
    }

    #[test]
    fn committed_delta_rejects_underflow_and_overflow_without_clamping() {
        let mut state = DiskAccountingState::default();
        state.committed_by_category.insert(DiskCategory::Wal, 5);

        assert!(state
            .apply_committed_delta(DiskCategory::Wal, 0, 6)
            .is_err());
        assert_eq!(state.category_bytes(DiskCategory::Wal), 5);

        state
            .committed_by_category
            .insert(DiskCategory::Wal, u64::MAX);
        assert!(state
            .apply_committed_delta(DiskCategory::Wal, 1, 0)
            .is_err());
        assert_eq!(state.category_bytes(DiskCategory::Wal), u64::MAX);
    }

    #[test]
    fn reservation_state_rejects_overflow_without_partial_mutation() {
        let mut state = DiskAccountingState {
            reserved_bytes: u64::MAX,
            active_reservations: 1,
            ..DiskAccountingState::default()
        };

        assert!(state.admit_reservation(1, false).is_err());
        assert_eq!(state.reserved_bytes, u64::MAX);
        assert_eq!(state.maintenance_reserved_bytes, 0);
        assert_eq!(state.active_reservations, 1);
    }

    #[test]
    fn category_total_rejects_cross_category_overflow() {
        let categories =
            BTreeMap::from([(DiskCategory::Wal, u64::MAX), (DiskCategory::Segments, 1)]);

        assert!(checked_category_total(&categories).is_err());
    }

    #[test]
    fn unbudgeted_group_stages_every_file_before_ordered_publication() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("control-log.json");
        let state_path = temp_dir.path().join("control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];

        with_staged_file_replacements(&replacements, |staged| {
            assert!(staged.temporary_path(0).unwrap().is_file());
            assert!(staged.temporary_path(1).unwrap().is_file());
            assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
            assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
            staged.publish(0)?;
            assert_eq!(fs::read(&log_path).unwrap(), b"new-log");
            assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
            staged.publish(1)
        })
        .unwrap();

        assert_eq!(fs::read(&log_path).unwrap(), b"new-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"new-state");
        assert_eq!(fs::read_dir(temp_dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn unbudgeted_group_cleans_owned_temps_when_publication_is_abandoned() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("control-log.json");
        let state_path = temp_dir.path().join("control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];

        let err = with_staged_file_replacements(&replacements, |_| -> Result<()> {
            Err(TsinkError::Other(
                "injected pre-publication failure".to_string(),
            ))
        })
        .unwrap_err();

        assert!(err.to_string().contains("injected pre-publication failure"));
        assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        assert_eq!(fs::read_dir(temp_dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn grouped_managed_replacements_reject_the_combined_peak_before_staging() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old").unwrap();
        fs::write(&state_path, b"old").unwrap();
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                // The complete pair plus its staged-entry allocation allowances must be rejected
                // before either temporary is written.
                max_bytes: Some(13),
                ..LocalDiskLimits::default()
            },
        )
        .unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"log!"),
            ManagedFileReplacement::new(&state_path, b"stat"),
        ];
        let requested = 8 + 2 * budget
            .snapshot_restore_entry_staging_allowance_bytes()
            .unwrap();
        let mut closure_invoked = false;

        let err = budget
            .with_staged_managed_file_replacements(&replacements, DiskCategory::Cluster, |_| {
                closure_invoked = true;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(
            err,
            TsinkError::DiskQuotaExceeded {
                limit: 13,
                used: 6,
                reserved: 0,
                requested: actual,
            } if actual == requested
        ));
        assert!(!closure_invoked);
        assert_eq!(fs::read(&log_path).unwrap(), b"old");
        assert_eq!(fs::read(&state_path).unwrap(), b"old");
        assert_eq!(fs::read_dir(&control_dir).unwrap().count(), 2);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 6);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn grouped_managed_replacements_reserve_missing_parents_before_creating_them() {
        let temp_dir = TempDir::new().unwrap();
        let target = temp_dir
            .path()
            .join("missing")
            .join("nested")
            .join("state.json");
        let replacement = [ManagedFileReplacement::new(&target, b"next")];
        let parent_allowance = crate::SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES;
        let requested = 4 + 3 * parent_allowance;
        let budget = budget_with_space(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(requested - 1),
                ..LocalDiskLimits::default()
            },
            u64::MAX,
        );
        let mut closure_invoked = false;

        let err = budget
            .with_staged_managed_file_replacements(&replacement, DiskCategory::Rollups, |_| {
                closure_invoked = true;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(
            err,
            TsinkError::DiskQuotaExceeded {
                limit,
                used: 0,
                reserved: 0,
                requested: actual,
            } if limit == requested - 1 && actual == requested
        ));
        assert!(!closure_invoked);
        assert!(!temp_dir.path().join("missing").exists());
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn grouped_prepublication_error_removes_owned_missing_parents() {
        let temp_dir = TempDir::new().unwrap();
        let target = temp_dir
            .path()
            .join("missing")
            .join("nested")
            .join("state.json");
        let replacement = [ManagedFileReplacement::new(&target, b"next")];
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();

        let err = budget
            .with_staged_managed_file_replacements(
                &replacement,
                DiskCategory::Rollups,
                |_| -> Result<()> {
                    Err(TsinkError::Other(
                        "injected prepublication rejection".to_string(),
                    ))
                },
            )
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("injected prepublication rejection"));
        assert!(!temp_dir.path().join("missing").exists());
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn grouped_managed_replacements_admit_staged_entry_allocation_units() {
        let temp_dir = TempDir::new().unwrap();
        let first = temp_dir.path().join("first.json");
        let second = temp_dir.path().join("second.json");
        let replacements = [
            ManagedFileReplacement::new(&first, b"a"),
            ManagedFileReplacement::new(&second, b"b"),
        ];
        let allowance = crate::SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES;
        let required = 2 + 2 * allowance;
        let budget = budget_with_space(temp_dir.path(), LocalDiskLimits::default(), required - 1);
        let mut closure_invoked = false;

        let err = budget
            .with_staged_managed_file_replacements(&replacements, DiskCategory::Rollups, |_| {
                closure_invoked = true;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(
            err,
            TsinkError::InsufficientDiskSpace {
                required: actual,
                available,
            } if actual == required && available == required - 1
        ));
        assert!(!closure_invoked);
        assert!(!first.exists());
        assert!(!second.exists());
        assert_eq!(fs::read_dir(temp_dir.path()).unwrap().count(), 0);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn grouped_managed_replacement_error_cleans_temps_and_preserves_typed_error() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];

        let err = budget
            .with_staged_managed_file_replacements(
                &replacements,
                DiskCategory::Cluster,
                |staged| -> Result<()> {
                    assert_eq!(staged.len(), 2);
                    assert!(staged.temporary_path(0).unwrap().is_file());
                    assert!(staged.temporary_path(1).unwrap().is_file());
                    Err(TsinkError::InsufficientDiskSpace {
                        required: 9,
                        available: 4,
                    })
                },
            )
            .unwrap_err();

        assert!(matches!(
            err,
            TsinkError::InsufficientDiskSpace {
                required: 9,
                available: 4,
            }
        ));
        assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        assert_eq!(fs::read_dir(&control_dir).unwrap().count(), 2);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 16);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.reconciliations_total, 1);
    }

    #[test]
    fn grouped_managed_replacement_stage_failure_cleans_prior_temps_and_reservation() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];
        let _write_failure = crate::engine::fs_utils::fail_tmp_write_after_bytes_once(
            fs::canonicalize(&state_path).unwrap(),
            3,
            std::io::ErrorKind::StorageFull,
            "injected grouped staging failure",
        );
        let mut closure_invoked = false;

        let err = budget
            .with_staged_managed_file_replacements(&replacements, DiskCategory::Cluster, |_| {
                closure_invoked = true;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(
            err,
            TsinkError::Io(ref source) if source.kind() == std::io::ErrorKind::StorageFull
        ));
        assert!(!closure_invoked);
        assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        assert_eq!(fs::read_dir(&control_dir).unwrap().count(), 2);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn grouped_staging_panic_cleans_owned_temp_without_leaking_reservation() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let original_bytes = u64::try_from(b"old-log".len() + b"old-state".len()).unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];
        let panic_guard = crate::engine::fs_utils::panic_tmp_write_after_bytes_once(
            fs::canonicalize(&state_path).unwrap(),
            3,
            "injected grouped staging panic",
        );

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = budget.with_staged_managed_file_replacements(
                &replacements,
                DiskCategory::Cluster,
                |_| Ok(()),
            );
        }));
        drop(panic_guard);

        assert!(unwind.is_err());
        assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        let owned_temps = fs::read_dir(&control_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".node.control-state.json.tmp-"))
            .collect::<Vec<_>>();
        assert!(owned_temps.is_empty());

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, original_bytes);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(budget.cleanup_atomic_write_temps(&state_path).unwrap(), 0);
    }

    #[test]
    fn unbudgeted_grouped_staging_panic_cleans_every_owned_temp() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("control-log.json");
        let state_path = temp_dir.path().join("control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];
        let panic_guard = crate::engine::fs_utils::panic_tmp_write_after_bytes_once(
            fs::canonicalize(&state_path).unwrap(),
            3,
            "injected unbudgeted grouped staging panic",
        );

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = with_staged_file_replacements(&replacements, |_| Ok(()));
        }));
        drop(panic_guard);

        assert!(unwind.is_err());
        assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        assert_eq!(fs::read_dir(temp_dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn grouped_managed_replacements_publish_sequentially_and_reconcile_exactly() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log-checkpoint"),
            ManagedFileReplacement::new(&state_path, b"new-state-mirror"),
        ];

        let value = budget
            .with_staged_managed_file_replacements(&replacements, DiskCategory::Cluster, |staged| {
                assert_eq!(staged.published_count(), 0);
                assert!(staged.temporary_path(0).unwrap().is_file());
                assert!(staged.temporary_path(1).unwrap().is_file());
                assert_eq!(fs::read(&log_path).unwrap(), b"old-log");
                assert_eq!(fs::read(&state_path).unwrap(), b"old-state");

                staged.publish(0)?;
                assert!(staged.is_published(0));
                assert!(!staged.is_published(1));
                assert_eq!(staged.published_count(), 1);
                assert_eq!(fs::read(&log_path).unwrap(), b"new-log-checkpoint");
                assert_eq!(fs::read(&state_path).unwrap(), b"old-state");

                staged.publish(1)?;
                assert_eq!(staged.published_count(), 2);
                assert_eq!(fs::read(&state_path).unwrap(), b"new-state-mirror");
                Ok("published")
            })
            .unwrap();

        assert_eq!(value, "published");
        assert_eq!(fs::read_dir(&control_dir).unwrap().count(), 2);
        let snapshot = budget.snapshot();
        let expected_bytes = (b"new-log-checkpoint".len() + b"new-state-mirror".len()) as u64;
        assert_eq!(snapshot.accounted_bytes, expected_bytes);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == DiskCategory::Cluster)
                .map(|usage| usage.bytes),
            Some(expected_bytes)
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.reconciliations_total, 2);
    }

    #[test]
    fn grouped_publication_records_rename_before_parent_sync_error() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            fs::canonicalize(&control_dir).unwrap(),
            "injected grouped log parent sync failure",
        );

        let err = budget
            .with_staged_managed_file_replacements(
                &replacements,
                DiskCategory::Cluster,
                |staged| -> Result<()> {
                    let err = staged.publish(0).unwrap_err();
                    assert!(staged.is_published(0));
                    assert_eq!(staged.published_count(), 1);
                    assert!(!staged.publication_ambiguous());
                    Err(err)
                },
            )
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("injected grouped log parent sync failure"));
        assert_eq!(fs::read(&log_path).unwrap(), b"new-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        assert_eq!(fs::read_dir(&control_dir).unwrap().count(), 2);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 16);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn grouped_publication_distinguishes_later_rename_failure() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"old-log").unwrap();
        fs::write(&state_path, b"old-state").unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default()).unwrap();
        let replacements = [
            ManagedFileReplacement::new(&log_path, b"new-log"),
            ManagedFileReplacement::new(&state_path, b"new-state"),
        ];

        let err = budget
            .with_staged_managed_file_replacements(
                &replacements,
                DiskCategory::Cluster,
                |staged| -> Result<()> {
                    staged.publish(0)?;
                    fs::remove_file(staged.temporary_path(1).unwrap()).unwrap();
                    let err = staged.publish(1).unwrap_err();
                    assert!(staged.is_published(0));
                    assert!(!staged.is_published(1));
                    assert_eq!(staged.published_count(), 1);
                    assert!(staged.publication_ambiguous());
                    Err(err)
                },
            )
            .unwrap_err();

        assert!(matches!(err, TsinkError::Io(_)));
        assert_eq!(fs::read(&log_path).unwrap(), b"new-log");
        assert_eq!(fs::read(&state_path).unwrap(), b"old-state");
        assert_eq!(fs::read_dir(&control_dir).unwrap().count(), 2);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 16);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn authoritative_grouped_recovery_can_repair_a_growing_mirror_above_quota() {
        let temp_dir = TempDir::new().unwrap();
        let control_dir = temp_dir.path().join("cluster/control");
        fs::create_dir_all(&control_dir).unwrap();
        let log_path = control_dir.join("node.control-log.json");
        let state_path = control_dir.join("node.control-state.json");
        fs::write(&log_path, b"authoritative-log").unwrap();
        fs::write(&state_path, b"x").unwrap();
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(5),
                ..LocalDiskLimits::default()
            },
        )
        .unwrap();
        assert!(budget.snapshot().over_limit);
        let replacements = [ManagedFileReplacement::new(&state_path, b"repaired-mirror")];

        let guarded_err = budget
            .with_staged_managed_file_replacements_for_recovery(
                &replacements,
                DiskCategory::Cluster,
                |staged| staged.publish(0),
            )
            .unwrap_err();
        assert!(matches!(guarded_err, TsinkError::InvalidConfiguration(_)));

        budget
            .with_staged_managed_file_replacements_for_authoritative_recovery(
                &replacements,
                DiskCategory::Cluster,
                |staged| staged.publish(0),
            )
            .unwrap();

        assert_eq!(fs::read(&state_path).unwrap(), b"repaired-mirror");
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 32);
        assert!(snapshot.over_limit);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    }

    #[test]
    #[should_panic(expected = "released bytes exceeded the local disk reservation total")]
    fn reservation_release_panics_instead_of_hiding_counter_underflow() {
        let mut state = DiskAccountingState {
            active_reservations: 1,
            ..DiskAccountingState::default()
        };

        state.release_reservation(1, false);
    }

    #[test]
    fn startup_reconciliation_counts_known_and_unknown_files_without_following_symlinks() {
        let temp = TempDir::new().unwrap();
        fs::create_dir_all(temp.path().join("wal")).unwrap();
        fs::write(temp.path().join("wal/wal-0001.log"), vec![1u8; 11]).unwrap();
        fs::write(temp.path().join("series_index.bin"), vec![2u8; 7]).unwrap();
        fs::write(temp.path().join("host-owned.bin"), vec![3u8; 13]).unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink("/", temp.path().join("external-link")).unwrap();

        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), u64::MAX);
        let snapshot = budget.snapshot();
        assert!(snapshot.accounted_bytes >= 31);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|entry| entry.category == DiskCategory::Wal)
                .map(|entry| entry.bytes),
            Some(11)
        );
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|entry| entry.category == DiskCategory::Registry)
                .map(|entry| entry.bytes),
            Some(7)
        );
        assert!(snapshot.unknown_bytes >= 13);
        assert_eq!(snapshot.reconciliations_total, 1);
    }

    #[test]
    fn exact_reconciliation_namespace_limit_counts_every_entry_globally() {
        let temp = TempDir::new().unwrap();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("host-owned"), b"one").unwrap();

        let categories =
            scan_path_with_namespace_limits(temp.path(), temp.path(), 2, 4, usize::MAX)
                .expect("the exact global cap must succeed");
        assert_eq!(checked_category_total(&categories).unwrap(), 3);

        fs::write(temp.path().join("nested/opaque"), b"two").unwrap();
        let err = scan_path_with_namespace_limits(temp.path(), temp.path(), 2, 4, usize::MAX)
            .expect_err("cap plus one must fail even for an unrecognized descendant");
        assert!(err.to_string().contains("2-entry global work bound"));
    }

    #[test]
    fn exact_reconciliation_namespace_limit_bounds_pending_depth() {
        let temp = TempDir::new().unwrap();
        fs::create_dir_all(temp.path().join("one/two")).unwrap();

        let err = scan_path_with_namespace_limits(temp.path(), temp.path(), 8, 1, usize::MAX)
            .expect_err("a directory beyond the recursive depth bound must fail");
        assert!(err.to_string().contains("1-level recursive depth bound"));
    }

    #[test]
    fn startup_reconciliation_memory_is_depth_bounded_and_exact_threshold_succeeds() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("data");
        fs::create_dir_all(&root).unwrap();
        for index in 0..4_096u32 {
            fs::write(root.join(format!("opaque-{index:04x}")), b"x").unwrap();
        }

        let mut exact_limit = 0usize;
        loop {
            match LocalDiskBudget::open_with_startup_memory_limit(
                &root,
                LocalDiskLimits::default(),
                exact_limit,
            ) {
                Ok(budget) => {
                    assert_eq!(budget.snapshot().accounted_bytes, 4_096);
                    break;
                }
                Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                    assert_eq!(budget, exact_limit);
                    assert!(required > exact_limit);
                    exact_limit = required;
                }
                Err(err) => panic!("unexpected bounded startup reconciliation error: {err}"),
            }
        }
        assert!(
            exact_limit < 8 * 1024,
            "sibling count leaked into streaming reconciliation peak: {exact_limit}"
        );
        let below = LocalDiskBudget::open_with_startup_memory_limit(
            &root,
            LocalDiskLimits::default(),
            exact_limit - 1,
        )
        .expect_err("one byte below the observed exact threshold must reject");
        assert!(matches!(
            below,
            TsinkError::MemoryBudgetExceeded {
                budget,
                required
            } if budget == exact_limit - 1 && required == exact_limit
        ));
        LocalDiskBudget::open_with_startup_memory_limit(
            &root,
            LocalDiskLimits::default(),
            exact_limit,
        )
        .expect("the exact modeled startup threshold must succeed");
    }

    #[test]
    fn concurrent_reservations_serialize_the_final_available_bytes() {
        let temp = TempDir::new().unwrap();
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(10),
                ..LocalDiskLimits::default()
            },
            u64::MAX,
        );
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                let reservation =
                    budget.reserve(DiskCategory::Wal, 10, DiskReservationKind::Growth);
                barrier.wait();
                reservation
            }));
        }
        barrier.wait();
        barrier.wait();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        drop(results);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn maintenance_reserve_is_unavailable_to_growth_but_available_to_maintenance() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("existing"), vec![0u8; 70]).unwrap();
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(100),
                maintenance_temp_reserve_bytes: 20,
                ..LocalDiskLimits::default()
            },
            1_000,
        );

        let growth = budget.reserve(DiskCategory::Segments, 11, DiskReservationKind::Growth);
        assert!(matches!(
            growth,
            Err(TsinkError::DiskQuotaExceeded {
                limit: 80,
                used: 70,
                reserved: 0,
                requested: 11
            })
        ));
        let maintenance = budget
            .reserve(
                DiskCategory::Temporary,
                30,
                DiskReservationKind::Maintenance,
            )
            .unwrap();
        maintenance.commit(0, 0).unwrap();
    }

    #[test]
    fn physical_headroom_accounts_for_live_reservations() {
        let temp = TempDir::new().unwrap();
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                filesystem_free_headroom_bytes: 100,
                maintenance_temp_reserve_bytes: 50,
                ..LocalDiskLimits::default()
            },
            200,
        );
        let first = budget
            .reserve(DiskCategory::Wal, 40, DiskReservationKind::Growth)
            .unwrap();
        let second = budget.reserve(DiskCategory::Wal, 11, DiskReservationKind::Growth);
        assert!(matches!(
            second,
            Err(TsinkError::InsufficientDiskSpace {
                required: 11,
                available: 10
            })
        ));
        drop(first);
    }

    #[test]
    fn recovery_reservations_bypass_logical_quota_but_honor_physical_headroom() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("existing"), vec![0u8; 15]).unwrap();
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(10),
                filesystem_free_headroom_bytes: 5,
                ..LocalDiskLimits::default()
            },
            10,
        );

        assert!(matches!(
            budget.reserve(DiskCategory::Wal, 1, DiskReservationKind::Growth),
            Err(TsinkError::DiskQuotaExceeded { .. })
        ));
        let recovery = budget
            .reserve(DiskCategory::Wal, 5, DiskReservationKind::Recovery)
            .unwrap();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.reserved_bytes, 5);
        assert_eq!(snapshot.maintenance_reserved_bytes, 5);
        assert_eq!(snapshot.active_reservations, 1);
        recovery.commit(0, 0).unwrap();

        assert!(matches!(
            budget.reserve(DiskCategory::Wal, 6, DiskReservationKind::Recovery),
            Err(TsinkError::InsufficientDiskSpace {
                required: 6,
                available: 5
            })
        ));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn recovery_append_and_shrinking_rewrite_restore_exact_accounting() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cluster/outbox.log");
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(4),
                ..LocalDiskLimits::default()
            },
            1_000,
        );

        budget
            .append_file_and_sync_parent(&path, b"1234", DiskCategory::Cluster)
            .unwrap();
        assert_eq!(budget.snapshot().accounted_bytes, 4);
        budget
            .append_file_and_sync_parent_for_recovery(&path, b"5", DiskCategory::Cluster)
            .unwrap();
        let over_limit = budget.snapshot();
        assert_eq!(over_limit.accounted_bytes, 5);
        assert!(over_limit.over_limit);

        budget
            .rewrite_file_atomically_and_sync_parent_for_recovery(
                &path,
                b"12",
                DiskCategory::Cluster,
            )
            .unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"12");
        let recovered = budget.snapshot();
        assert_eq!(recovered.accounted_bytes, 2);
        assert!(!recovered.over_limit);
        assert_eq!(recovered.reserved_bytes, 0);
        assert_eq!(recovered.active_reservations, 0);
        assert_eq!(
            recovered
                .categories
                .iter()
                .find(|usage| usage.category == DiskCategory::Cluster)
                .map(|usage| usage.bytes),
            Some(2)
        );
    }

    #[test]
    fn recovery_rewrite_rejects_growth_without_mutating_file_or_accounting() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cluster/outbox.log");
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(8),
                ..LocalDiskLimits::default()
            },
            1_000,
        );
        budget
            .append_file_and_sync_parent(&path, b"1234", DiskCategory::Cluster)
            .unwrap();
        let before = budget.snapshot();

        let err = budget
            .rewrite_file_atomically_and_sync_parent_for_recovery(
                &path,
                b"12345",
                DiskCategory::Cluster,
            )
            .unwrap_err();

        assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
        assert_eq!(fs::read(&path).unwrap(), b"1234");
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
        assert_eq!(after.categories, before.categories);
    }

    #[test]
    fn streamed_cleanup_rewrite_supports_bounded_chunks_and_exact_accounting() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cluster/outbox.log");
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(64),
                ..LocalDiskLimits::default()
            },
            1_000,
        );
        budget
            .append_file_and_sync_parent(&path, b"0123456789", DiskCategory::Cluster)
            .unwrap();

        budget
            .rewrite_file_atomically_and_sync_parent_for_cleanup_with(
                &path,
                6,
                DiskCategory::Cluster,
                |writer| {
                    writer.write_all(b"ab")?;
                    writer.write_all(b"cd")?;
                    writer.write_all(b"ef")?;
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"abcdef");
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 6);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == DiskCategory::Cluster)
                .map(|usage| usage.bytes),
            Some(6)
        );
    }

    #[test]
    fn streamed_cleanup_rewrite_rejects_declared_length_overrun_without_publication() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cluster/outbox.log");
        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), 1_000);
        budget
            .append_file_and_sync_parent(&path, b"old-state", DiskCategory::Cluster)
            .unwrap();
        let before = budget.snapshot();

        let err = budget
            .rewrite_file_atomically_and_sync_parent_for_cleanup_with(
                &path,
                3,
                DiskCategory::Cluster,
                |writer| {
                    writer.write_all(b"four")?;
                    Ok(())
                },
            )
            .unwrap_err();

        assert!(err.to_string().contains("exceeded its declared length"));
        assert_eq!(fs::read(&path).unwrap(), b"old-state");
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.categories, before.categories);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn recovery_append_can_consume_physical_maintenance_reserve() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cluster/outbox.log");
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                filesystem_free_headroom_bytes: 100,
                maintenance_temp_reserve_bytes: 50,
                ..LocalDiskLimits::default()
            },
            200,
        );

        assert!(matches!(
            budget.append_file_and_sync_parent(&path, &[0; 60], DiskCategory::Cluster),
            Err(TsinkError::InsufficientDiskSpace {
                required: 60,
                available: 50
            })
        ));
        budget
            .append_file_and_sync_parent_for_recovery(&path, &[0; 60], DiskCategory::Cluster)
            .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 60);
        assert_eq!(budget.snapshot().accounted_bytes, 60);
    }

    #[test]
    fn commit_tracks_final_delta_and_deletion_allows_over_limit_recovery() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("unknown");
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&[0u8; 15]).unwrap();
        let budget = budget_with_space(
            temp.path(),
            LocalDiskLimits {
                max_bytes: Some(10),
                ..LocalDiskLimits::default()
            },
            1_000,
        );
        assert!(budget.snapshot().over_limit);
        assert!(budget
            .reserve(DiskCategory::Unknown, 1, DiskReservationKind::Growth)
            .is_err());
        fs::write(&path, vec![0u8; 9]).unwrap();
        budget.reconcile().unwrap();
        assert!(!budget.snapshot().over_limit);

        let reservation = budget
            .reserve(DiskCategory::Unknown, 1, DiskReservationKind::Growth)
            .unwrap();
        reservation.commit(1, 0).unwrap();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 10);
        assert_eq!(snapshot.reservation_overruns_total, 0);
    }

    #[test]
    fn governed_paths_resolve_relative_components_and_missing_descendants() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("data");
        let budget = budget_with_space(&root, LocalDiskLimits::default(), 1_000);

        assert!(budget.governs(&root).unwrap());
        assert!(budget.governs(&root.join("missing/child")).unwrap());
        assert!(!budget.governs(&root.join("../outside")).unwrap());
        assert!(budget.overlaps(&root.join("missing/child")).unwrap());
        assert!(budget.overlaps(temp.path()).unwrap());
        assert!(!budget.overlaps(&temp.path().join("sibling")).unwrap());

        #[cfg(unix)]
        {
            let outside = temp.path().join("outside");
            fs::create_dir_all(&outside).unwrap();
            std::os::unix::fs::symlink(&root, temp.path().join("data-alias")).unwrap();
            std::os::unix::fs::symlink(&outside, root.join("outside-alias")).unwrap();

            assert!(budget
                .governs(&temp.path().join("data-alias/missing"))
                .unwrap());
            assert!(!budget.governs(&root.join("outside-alias/missing")).unwrap());
        }
    }

    #[test]
    fn budget_open_retries_sync_for_every_configured_root_ancestor() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let base = fs::canonicalize(temp_dir.path()).expect("temp root should canonicalize");
        let first_missing_ancestor = base.join("nested-root");
        let root = first_missing_ancestor.join("deeper/data");

        {
            let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
                first_missing_ancestor.clone(),
                "injected configured-root ancestor sync failure",
            );
            LocalDiskBudget::open(&root, LocalDiskLimits::default())
                .expect_err("opening should fail when a configured-root ancestor cannot sync");
        }
        assert!(root.is_dir());

        {
            let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
                first_missing_ancestor,
                "injected configured-root retry sync failure",
            );
            LocalDiskBudget::open(&root, LocalDiskLimits::default()).expect_err(
                "retry must resynchronize every ancestor even after all entries already exist",
            );
        }
        LocalDiskBudget::open(&root, LocalDiskLimits::default())
            .expect("opening should succeed after the full configured-root ancestry syncs");
    }

    #[test]
    fn durable_directory_creation_retries_ancestor_syncs_after_entries_exist() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let root = temp_dir.path().join("data");
        let budget = LocalDiskBudget::open(&root, LocalDiskLimits::default())
            .expect("disk budget should open");
        let nested = root.join("usage/ledger");
        let synchronized_root = budget.root().to_path_buf();

        {
            let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
                synchronized_root.clone(),
                "injected ancestor sync failure",
            );
            budget
                .create_dir_all_and_sync_parents(&nested)
                .expect_err("first ancestor sync should fail after creating the directories");
        }
        assert!(nested.is_dir());

        {
            let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
                synchronized_root,
                "injected retry sync failure",
            );
            budget
                .create_dir_all_and_sync_parents(&nested)
                .expect_err("retry must resynchronize an already-existing ancestor entry");
        }
        budget
            .create_dir_all_and_sync_parents(&nested)
            .expect("a later complete sync should make the directory chain durable");
    }

    #[test]
    fn failed_atomic_rollback_cannot_clobber_a_concurrent_successful_replacement() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let target = temp_dir.path().join("managed-state.json");
        fs::write(&target, b"old-state").expect("old state should write");
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");
        let first_sync = Arc::new(AtomicBool::new(true));
        let failed_writer_entered = Arc::new(Barrier::new(2));
        let release_failed_writer = Arc::new(Barrier::new(2));
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
            {
                let root = temp_dir.path().to_path_buf();
                let first_sync = Arc::clone(&first_sync);
                let failed_writer_entered = Arc::clone(&failed_writer_entered);
                let release_failed_writer = Arc::clone(&release_failed_writer);
                move |candidate| {
                    if candidate == root.as_path() && first_sync.swap(false, Ordering::SeqCst) {
                        failed_writer_entered.wait();
                        release_failed_writer.wait();
                        true
                    } else {
                        false
                    }
                }
            },
            "injected post-rename sync failure",
        );

        let failed_budget = Arc::clone(&budget);
        let failed_target = target.clone();
        let failed_writer = std::thread::spawn(move || {
            failed_budget.write_file_atomically_and_sync_parent(
                &failed_target,
                b"failed-state",
                DiskCategory::Metadata,
            )
        });
        failed_writer_entered.wait();

        let successful_budget = Arc::clone(&budget);
        let successful_target = target.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (successful_tx, successful_rx) = std::sync::mpsc::sync_channel(1);
        let successful_writer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = successful_budget.write_file_atomically_and_sync_parent(
                &successful_target,
                b"successful-state",
                DiskCategory::Metadata,
            );
            successful_tx.send(result).unwrap();
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second writer should reach the managed mutation call");
        let mutation_lock_was_held = budget.managed_file_mutation_lock.try_lock().is_none();
        let early_result = successful_rx.recv_timeout(Duration::from_millis(250));

        release_failed_writer.wait();
        failed_writer
            .join()
            .expect("failed writer should not panic")
            .expect_err("the injected sync failure should be returned after rollback");
        let second_writer_timed_out = matches!(
            &early_result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        );
        let successful_result = match early_result {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                successful_rx.recv_timeout(Duration::from_secs(1)).expect(
                    "successful writer should finish after rollback releases the mutation lock",
                )
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("successful writer disconnected before reporting its result")
            }
        };
        successful_writer
            .join()
            .expect("successful writer should not panic");
        assert!(
            mutation_lock_was_held,
            "failed writer must hold the shared mutation lock through rollback"
        );
        assert!(
            second_writer_timed_out,
            "second writer completed before the failed writer released rollback serialization"
        );
        successful_result.expect("second replacement should succeed");
        assert_eq!(
            fs::read(&target).expect("final managed state should remain readable"),
            b"successful-state"
        );
    }

    #[test]
    fn managed_atomic_write_rolls_back_after_post_rename_sync_failure() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let target = temp_dir.path().join("metric-metadata-store.json");
        fs::write(&target, b"old-state").expect("old state should write");
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            temp_dir.path().to_path_buf(),
            "injected sidecar parent sync failure",
        );

        let err = budget
            .write_file_atomically_and_sync_parent(&target, b"new-state", DiskCategory::Metadata)
            .expect_err("post-rename sync failure should be reported");

        assert!(err
            .to_string()
            .contains("injected sidecar parent sync failure"));
        assert_eq!(fs::read(&target).unwrap(), b"old-state");
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 9);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn atomic_temp_cleanup_removes_only_the_owned_exact_prefix() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let target = temp_dir.path().join("rules-store.json");
        let orphan = temp_dir
            .path()
            .join(".rules-store.json.tmp-123-0000000000000001");
        let unrelated = temp_dir.path().join(".other.json.tmp-123-1");
        let malformed_owned_prefix = temp_dir.path().join(".rules-store.json.tmp-manual");
        let uppercase_nonce = temp_dir
            .path()
            .join(".rules-store.json.tmp-123-000000000000000A");
        let leading_zero_pid = temp_dir
            .path()
            .join(".rules-store.json.tmp-0123-0000000000000002");
        let overflowing_pid = temp_dir
            .path()
            .join(".rules-store.json.tmp-4294967296-0000000000000003");
        fs::write(&orphan, b"orphan").expect("orphan should write");
        fs::write(&unrelated, b"host-owned").expect("unrelated file should write");
        fs::write(&malformed_owned_prefix, b"operator-owned").expect("lookalike should write");
        fs::write(&uppercase_nonce, b"uppercase").expect("uppercase lookalike should write");
        fs::write(&leading_zero_pid, b"leading-zero").expect("PID lookalike should write");
        fs::write(&overflowing_pid, b"overflowing-pid")
            .expect("out-of-range PID lookalike should write");
        let expected_remaining = [
            &unrelated,
            &malformed_owned_prefix,
            &uppercase_nonce,
            &leading_zero_pid,
            &overflowing_pid,
        ]
        .into_iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum::<u64>();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");

        assert_eq!(budget.cleanup_atomic_write_temps(&target).unwrap(), 1);
        assert!(!orphan.exists());
        assert_eq!(fs::read(&unrelated).unwrap(), b"host-owned");
        assert_eq!(
            fs::read(&malformed_owned_prefix).unwrap(),
            b"operator-owned"
        );
        assert!(uppercase_nonce.is_file());
        assert!(leading_zero_pid.is_file());
        assert!(overflowing_pid.is_file());
        assert_eq!(budget.snapshot().accounted_bytes, expected_remaining);
    }

    #[test]
    fn atomic_temp_cleanup_noop_does_not_rescan_the_managed_tree() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let target = temp_dir.path().join("rules-store.json");
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");
        assert_eq!(budget.snapshot().reconciliations_total, 1);

        assert_eq!(budget.cleanup_atomic_write_temps(&target).unwrap(), 0);
        assert_eq!(budget.snapshot().reconciliations_total, 1);
    }

    #[test]
    fn recursive_owned_temp_cleanup_preflights_all_candidates_before_deletion() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let exact = temp_dir.path().join("owned-exact");
        let first = temp_dir.path().join("owned-first");
        let second = temp_dir.path().join("owned-second");
        fs::create_dir_all(&exact).unwrap();
        fs::write(exact.join("one"), b"one").unwrap();
        fs::write(exact.join("two"), b"two").unwrap();
        fs::create_dir_all(&first).unwrap();
        fs::write(first.join("one"), b"one").unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(second.join("two"), b"two").unwrap();
        fs::write(second.join("three"), b"three").unwrap();
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");

        assert_eq!(
            budget
                .cleanup_owned_temporary_entries_matching_with_namespace_limits(
                    temp_dir.path(),
                    true,
                    |name| name == "owned-exact",
                    "test recursive cleanup",
                    8,
                    2,
                    4,
                    usize::MAX,
                    true,
                )
                .expect("the exact recursive cap must succeed"),
            1
        );
        assert!(!exact.exists());

        let err = budget
            .cleanup_owned_temporary_entries_matching_with_namespace_limits(
                temp_dir.path(),
                true,
                |name| matches!(name, "owned-first" | "owned-second"),
                "test recursive cleanup",
                8,
                2,
                4,
                usize::MAX,
                true,
            )
            .expect_err("cap plus one across candidates must fail closed");
        assert!(err.to_string().contains("2-entry global work bound"));
        assert_eq!(fs::read(first.join("one")).unwrap(), b"one");
        assert_eq!(fs::read(second.join("two")).unwrap(), b"two");
        assert_eq!(fs::read(second.join("three")).unwrap(), b"three");
    }

    #[cfg(unix)]
    #[test]
    fn expected_managed_entry_cannot_escape_through_a_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().expect("temp dir should build");
        let data_root = temp_dir.path().join("data");
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(&data_root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&outside, data_root.join("lane_numeric")).unwrap();
        let budget = LocalDiskBudget::open(&data_root, LocalDiskLimits::default())
            .expect("disk budget should open");

        let escaped = data_root.join("lane_numeric/segments/L0/seg-0000000000000001");
        let err = budget
            .governs_entry(&escaped)
            .expect_err("a lexically managed entry must not silently escape the budget root");
        assert!(matches!(err, TsinkError::InvalidConfiguration(message)
            if message.contains("escapes local disk root")));
        assert!(!budget.governs_entry(&outside.join("remote-file")).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn managed_append_rejects_a_dangling_final_symlink() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().expect("temp dir should build");
        let data_root = temp_dir.path().join("data");
        let outside = temp_dir.path().join("outside-ledger.ndjson");
        fs::create_dir_all(data_root.join("usage-accounting")).unwrap();
        let ledger = data_root.join("usage-accounting/ledger.ndjson");
        symlink(&outside, &ledger).expect("dangling symlink should build");
        let budget = LocalDiskBudget::open(&data_root, LocalDiskLimits::default())
            .expect("disk budget should open");

        let err = budget
            .append_file_and_sync_parent(&ledger, b"record\n", DiskCategory::ServerState)
            .expect_err("managed append must reject the final symlink");
        assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
        assert!(!outside.exists());
        assert_eq!(budget.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn reconciliation_rejects_live_reservations() {
        let temp = TempDir::new().unwrap();
        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), 1_000);
        let reservation = budget
            .reserve(DiskCategory::Wal, 1, DiskReservationKind::Growth)
            .unwrap();

        let err = budget.reconcile().unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot reconcile local disk usage while 1 reservation(s) are active"));
        drop(reservation);
        budget.reconcile().unwrap();
    }

    #[test]
    fn idle_reconciliation_waits_for_an_unrelated_live_reservation() {
        let temp = TempDir::new().unwrap();
        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), 1_000);
        let reservation = budget
            .reserve(DiskCategory::Wal, 1, DiskReservationKind::Growth)
            .unwrap();
        let waiting_budget = Arc::clone(&budget);
        let waiter = std::thread::spawn(move || waiting_budget.reconcile_when_idle());

        while budget.state.lock().reconciliation_waiters == 0 {
            std::thread::yield_now();
        }
        assert!(!waiter.is_finished());
        drop(reservation);

        let snapshot = waiter.join().unwrap().unwrap();
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.reconciliations_total, 2);
    }

    #[test]
    fn reservation_overrun_is_reported_after_surviving_bytes_are_accounted() {
        let temp = TempDir::new().unwrap();
        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), 1_000);
        let reservation = budget
            .reserve(DiskCategory::Wal, 5, DiskReservationKind::Growth)
            .unwrap();

        assert!(matches!(
            reservation.commit(7, 0),
            Err(TsinkError::InsufficientDiskSpace {
                required: 7,
                available: 5
            })
        ));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 7);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reservation_overruns_total, 1);
    }

    #[test]
    fn reconciliation_classifies_only_owned_segment_layouts_as_segments() {
        let temp = TempDir::new().unwrap();
        let segment_file = temp
            .path()
            .join("lane_numeric/segments/L0/seg-0000000000000001/chunks.bin");
        fs::create_dir_all(segment_file.parent().unwrap()).unwrap();
        fs::write(&segment_file, vec![1u8; 5]).unwrap();
        fs::write(temp.path().join("lane_numeric/host-file.bin"), vec![2u8; 9]).unwrap();
        fs::write(
            temp.path().join("lane_numeric/tombstones.json"),
            vec![3u8; 7],
        )
        .unwrap();

        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), 1_000);
        let snapshot = budget.snapshot();
        let category_bytes = |category| {
            snapshot
                .categories
                .iter()
                .find(|entry| entry.category == category)
                .map(|entry| entry.bytes)
                .unwrap_or(0)
        };
        assert_eq!(category_bytes(DiskCategory::Segments), 5);
        assert_eq!(category_bytes(DiskCategory::Tombstones), 7);
        assert_eq!(category_bytes(DiskCategory::Unknown), 9);
    }

    #[test]
    fn reconciled_operation_reservation_conservatively_settles_on_unwind() {
        let temp = TempDir::new().unwrap();
        let budget = budget_with_space(temp.path(), LocalDiskLimits::default(), 1_000_000);
        let peak_bytes = 4096;
        let survivor = temp.path().join("survivor.bin");

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = budget.with_reconciled_maintenance_reservation(
                DiskCategory::Temporary,
                peak_bytes,
                || -> Result<()> {
                    fs::write(&survivor, b"x").unwrap();
                    panic!("injected aggregate-operation unwind");
                },
            );
        }));

        assert!(unwind.is_err());
        let conservative = budget.snapshot();
        assert_eq!(conservative.active_reservations, 0);
        assert_eq!(conservative.reserved_bytes, 0);
        assert_eq!(conservative.maintenance_reserved_bytes, 0);
        assert_eq!(conservative.accounted_bytes, peak_bytes);
        assert_eq!(budget.reconcile().unwrap().accounted_bytes, 1);
    }
}
