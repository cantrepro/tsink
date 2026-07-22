//! Shared local-disk accounting and reservation primitives.
//!
//! A budget covers one local data-directory tree. It counts regular files and symlinks without
//! following symlinks, keeps unknown files in the total, and serializes reservations so concurrent
//! writers cannot all admit against the same remaining capacity. Reservations are conservative:
//! writers reserve their peak additional footprint and commit only the final persistent delta.

use crate::{Result, TsinkError};
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
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

trait SpaceProbe: Send + Sync {
    fn available_space(&self, path: &Path) -> std::io::Result<u64>;
}

#[derive(Debug)]
struct SystemSpaceProbe;

impl SpaceProbe for SystemSpaceProbe {
    fn available_space(&self, path: &Path) -> std::io::Result<u64> {
        system_available_space(path)
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
    root: PathBuf,
    limits: LocalDiskLimits,
    state: Mutex<DiskAccountingState>,
    reservations_released: Condvar,
    space_probe: Arc<dyn SpaceProbe>,
}

impl fmt::Debug for LocalDiskBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDiskBudget")
            .field("root", &self.root)
            .field("limits", &self.limits)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl LocalDiskBudget {
    /// Opens a budget, creates the root when necessary, and reconciles its current contents.
    pub fn open(root: impl AsRef<Path>, limits: LocalDiskLimits) -> Result<Arc<Self>> {
        Self::open_with_space_probe(root.as_ref(), limits, Arc::new(SystemSpaceProbe))
    }

    fn open_with_space_probe(
        root: &Path,
        limits: LocalDiskLimits,
        space_probe: Arc<dyn SpaceProbe>,
    ) -> Result<Arc<Self>> {
        let limits = limits.validate()?;
        fs::create_dir_all(root).map_err(|source| TsinkError::IoWithPath {
            path: root.to_path_buf(),
            source,
        })?;
        let root = fs::canonicalize(root).map_err(|source| TsinkError::IoWithPath {
            path: root.to_path_buf(),
            source,
        })?;
        let budget = Arc::new(Self {
            root,
            limits,
            state: Mutex::new(DiskAccountingState::default()),
            reservations_released: Condvar::new(),
            space_probe,
        });
        budget.reconcile()?;
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

    /// Returns whether `path` resolves to the managed root or one of its descendants.
    ///
    /// Existing symlinks are resolved component by component. Missing final components are
    /// normalized lexically, so this remains safe for destinations that have not been created.
    pub fn governs(&self, path: &Path) -> Result<bool> {
        Ok(resolve_path_allow_missing(path)?.starts_with(&self.root))
    }

    /// Returns whether the directory entry at `path` belongs to the managed tree.
    ///
    /// Unlike [`LocalDiskBudget::governs`], this resolves the parent but deliberately does not
    /// follow the final component. Filesystem mutators use this because replacing or removing an
    /// in-tree symlink mutates the managed directory entry even when its target is outside.
    pub(crate) fn governs_entry(&self, path: &Path) -> Result<bool> {
        let Some(file_name) = path.file_name() else {
            return self.matches_root(path);
        };
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        Ok(resolve_path_allow_missing(parent)?
            .join(file_name)
            .starts_with(&self.root))
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
        let mut state = self.state.lock();
        if state.active_reservations > 0 {
            return Err(TsinkError::Other(format!(
                "cannot reconcile local disk usage while {} reservation(s) are active",
                state.active_reservations
            )));
        }
        let categories = scan_tree(&self.root)?;
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
    pub(crate) fn reconcile_when_idle(&self) -> Result<LocalDiskBudgetSnapshot> {
        let mut state = self.state.lock();
        state.reconciliation_waiters =
            state.reconciliation_waiters.checked_add(1).ok_or_else(|| {
                TsinkError::Other("local disk reconciliation waiter counter overflow".to_string())
            })?;
        while state.active_reservations > 0 {
            self.reservations_released.wait(&mut state);
        }

        let categories_result = scan_tree(&self.root);
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
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        let categories = scan_tree(path)?;
        return checked_category_total(&categories);
    }
    if metadata.is_file() || metadata.file_type().is_symlink() {
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
    scan_path(root, root)
}

fn scan_path(root: &Path, path: &Path) -> Result<BTreeMap<DiskCategory, u64>> {
    let mut categories = BTreeMap::new();
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(categories),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if metadata.is_file() || metadata.file_type().is_symlink() {
        categories.insert(classify_path(root, path), metadata.len());
        return Ok(categories);
    }
    if !metadata.is_dir() {
        return Ok(categories);
    }

    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).map_err(|source| TsinkError::IoWithPath {
            path: directory.clone(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: directory.clone(),
                source,
            })?;
            let path = entry.path();
            let metadata =
                fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                    path: path.clone(),
                    source,
                })?;
            let file_type = metadata.file_type();
            if file_type.is_dir() && !file_type.is_symlink() {
                pending.push(path);
                continue;
            }
            if file_type.is_file() || file_type.is_symlink() {
                let category = classify_path(root, &path);
                let bytes = categories.entry(category).or_insert(0u64);
                *bytes = bytes.checked_add(metadata.len()).ok_or_else(|| {
                    TsinkError::Other(format!(
                        "local disk accounting overflow while scanning category {category:?} at {}",
                        path.display()
                    ))
                })?;
            }
        }
    }
    Ok(categories)
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

fn classify_path(root: &Path, path: &Path) -> DiskCategory {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let components: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_ascii_lowercase())
        .collect();
    let file_name = components.last().map(String::as_str).unwrap_or_default();

    if components.iter().any(|component| {
        component == ".compaction-replacements"
            || component.starts_with(".tmp-tsink-")
            || component.starts_with(".tmp-seg-")
            || (component.starts_with('.') && component.contains(".tmp-"))
    }) || file_name == "wal.published.tmp"
    {
        return DiskCategory::Temporary;
    }
    if components
        .first()
        .is_some_and(|component| component == "wal")
    {
        return DiskCategory::Wal;
    }
    if file_name == "tombstones.json"
        || components
            .iter()
            .any(|component| component == "tombstones.json.store")
    {
        return DiskCategory::Tombstones;
    }
    if file_name == "series_index.bin"
        || file_name == "series_index.delta.bin"
        || file_name == "series_index.catalog.json"
        || file_name == "segment_catalog.json"
        || components
            .iter()
            .any(|component| component == "series_index.delta.d")
    {
        return DiskCategory::Registry;
    }
    if components.len() >= 5
        && matches!(components[0].as_str(), "lane_numeric" | "lane_blob")
        && components[1] == "segments"
        && components[2].strip_prefix('l').is_some_and(|level| {
            !level.is_empty() && level.bytes().all(|byte| byte.is_ascii_digit())
        })
        && components[3]
            .strip_prefix("seg-")
            .is_some_and(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return DiskCategory::Segments;
    }
    if components
        .first()
        .is_some_and(|component| component == ".rollups")
    {
        return DiskCategory::Rollups;
    }
    if components
        .first()
        .is_some_and(|component| component == "edge_sync")
    {
        return DiskCategory::EdgeSync;
    }
    if components.iter().any(|component| {
        component.contains("cluster")
            || component.contains("outbox")
            || component.contains("dedupe")
            || component.contains("audit")
            || component.contains("consensus")
    }) {
        return DiskCategory::Cluster;
    }
    if file_name == "metric-metadata-store.json" {
        return DiskCategory::Metadata;
    }
    if file_name == "exemplar-store.json" {
        return DiskCategory::Exemplars;
    }
    if file_name == "rules-store.json"
        || components.iter().any(|component| {
            component == "usage-accounting" || component == "managed-control-plane"
        })
    {
        return DiskCategory::ServerState;
    }
    DiskCategory::Unknown
}

#[cfg(unix)]
fn system_available_space(path: &Path) -> std::io::Result<u64> {
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
    let stats = unsafe { stats.assume_init() };
    let fragment_size = if stats.f_frsize == 0 {
        stats.f_bsize
    } else {
        stats.f_frsize
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
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
}
