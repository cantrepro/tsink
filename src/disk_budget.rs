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
    configured_root: PathBuf,
    root: PathBuf,
    limits: LocalDiskLimits,
    state: Mutex<DiskAccountingState>,
    managed_file_mutation_lock: Mutex<()>,
    reservations_released: Condvar,
    space_probe: Arc<dyn SpaceProbe>,
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
        Self::open_with_space_probe(root.as_ref(), limits, Arc::new(SystemSpaceProbe))
    }

    fn open_with_space_probe(
        root: &Path,
        limits: LocalDiskLimits,
        space_probe: Arc<dyn SpaceProbe>,
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
            Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
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
            true,
            false,
            |candidate| atomic_write_temp_target_name(candidate) == Some(target_name),
            "atomic-write temporary cleanup",
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
    pub(crate) fn cleanup_atomic_write_temps_matching_targets<F>(
        self: &Arc<Self>,
        directory: &Path,
        owns_target: F,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching(
            directory,
            false,
            false,
            |candidate| atomic_write_temp_target_name(candidate).is_some_and(&owns_target),
            "owned atomic-write temporary cleanup",
        )
    }

    /// Removes explicitly-owned temporary directory entries. Regular files and symlinks with an
    /// owned name are also safe to unlink; special file types are rejected.
    pub(crate) fn cleanup_temporary_directories_matching_names<F>(
        self: &Arc<Self>,
        directory: &Path,
        owns_name: F,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        self.cleanup_owned_temporary_entries_matching(
            directory,
            false,
            true,
            owns_name,
            "owned temporary-directory cleanup",
        )
    }

    fn cleanup_owned_temporary_entries_matching<F>(
        self: &Arc<Self>,
        directory: &Path,
        create_directory: bool,
        allow_directories: bool,
        owns_name: F,
        operation: &str,
    ) -> Result<u64>
    where
        F: Fn(&str) -> bool,
    {
        let _mutation_guard = self.managed_file_mutation_lock.lock();
        self.validate_managed_directory_path(directory)?;
        if create_directory {
            self.create_dir_all_and_sync_parents(directory)?;
        } else if !crate::engine::fs_utils::path_exists_no_follow(directory)? {
            return Ok(0);
        }

        let mut owned_entries = Vec::new();
        for entry in fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: directory.to_path_buf(),
                source,
            })?;
            let entry_name = entry.file_name();
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
            if !(file_type.is_file()
                || file_type.is_symlink()
                || (file_type.is_dir() && allow_directories))
            {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "refusing to remove ambiguous owned temporary entry: {}",
                    entry_path.display()
                )));
            }
            owned_entries.push(entry_path);
        }
        if owned_entries.is_empty() {
            return Ok(0);
        }

        let reservation =
            self.reserve(DiskCategory::Temporary, 0, DiskReservationKind::Recovery)?;
        let mut removed = 0u64;
        let mut cleanup_error = None;
        for entry_path in owned_entries {
            if let Err(err) = crate::engine::fs_utils::remove_path_if_exists(&entry_path) {
                cleanup_error = Some(err);
                break;
            }
            removed = removed.saturating_add(1);
        }
        if removed > 0 {
            if let Err(err) = crate::engine::fs_utils::sync_dir(directory) {
                cleanup_error.get_or_insert(err);
            }
        }
        let settlement_result = reservation.commit(0, 0);
        let reconciliation_result = self.reconcile_when_idle().map(|_| ());
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
    pub fn reconcile_when_idle(&self) -> Result<LocalDiskBudgetSnapshot> {
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
}
