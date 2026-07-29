use std::fs::OpenOptions;
use std::io::{BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{Result, TsinkError};

#[path = "fs_utils/secure_snapshot.rs"]
mod secure_snapshot;
pub(crate) use secure_snapshot::{
    admit_secure_snapshot_operation_retained_bytes, attest_secure_snapshot_requested_path_absent,
    SecureSnapshotNamespaceFence, SecureSnapshotSourceFile, SecureSnapshotSourceTree,
    SecureSnapshotStagingDirectory,
};

static STAGE_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Recovery-owned namespace scans must complete within a fixed work envelope before callers
/// delete any discovered entry. Count every directory entry, including unknown names, so an
/// attacker cannot hide unbounded work behind lookalikes that the caller later ignores.
pub(crate) const MAX_RECOVERY_NAMESPACE_ENTRIES: usize = 16_384;
pub(crate) const MAX_RECOVERY_NAMESPACE_DEPTH: u32 = 128;

/// One entry counter shared across every directory participating in a recovery scan.
///
/// Callers that inspect several sibling directories must reuse the same value so the configured
/// ceiling is global rather than silently resetting for each directory.
#[derive(Debug)]
pub(crate) struct RecoveryNamespaceBudget {
    max_entries: usize,
    observed_entries: usize,
}

impl RecoveryNamespaceBudget {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            observed_entries: 0,
        }
    }

    pub(crate) fn collect_directory_entries(
        &mut self,
        directory: &Path,
        operation: &str,
    ) -> Result<Vec<std::fs::DirEntry>> {
        let remaining = self.max_entries.saturating_sub(self.observed_entries);
        let mut entries = Vec::new();
        entries.try_reserve(remaining.min(1024)).map_err(|_| {
            TsinkError::Other(format!(
                "unable to allocate bounded directory scan for {operation}: {}",
                directory.display()
            ))
        })?;
        for entry in std::fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })? {
            if self.observed_entries == self.max_entries {
                return Err(TsinkError::DataCorruption(format!(
                    "{operation} exceeds its {}-entry global work bound: {}",
                    self.max_entries,
                    directory.display()
                )));
            }
            entries.push(entry.map_err(|source| TsinkError::IoWithPath {
                path: directory.to_path_buf(),
                source,
            })?);
            self.observed_entries = self.observed_entries.checked_add(1).ok_or_else(|| {
                TsinkError::Other(format!(
                    "{operation} namespace entry counter overflow at {}",
                    directory.display()
                ))
            })?;
        }
        Ok(entries)
    }

    /// Charges one streamed entry without retaining the directory's remaining siblings.
    pub(crate) fn observe_entry(&mut self, directory: &Path, operation: &str) -> Result<()> {
        if self.observed_entries == self.max_entries {
            return Err(TsinkError::DataCorruption(format!(
                "{operation} exceeds its {}-entry global work bound: {}",
                self.max_entries,
                directory.display()
            )));
        }
        self.observed_entries = self.observed_entries.checked_add(1).ok_or_else(|| {
            TsinkError::Other(format!(
                "{operation} namespace entry counter overflow at {}",
                directory.display()
            ))
        })?;
        Ok(())
    }
}

pub(crate) fn collect_directory_entries_bounded(
    directory: &Path,
    max_entries: usize,
    operation: &str,
) -> Result<Vec<std::fs::DirEntry>> {
    RecoveryNamespaceBudget::new(max_entries).collect_directory_entries(directory, operation)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannedRemovalKind {
    Directory,
    FileLike,
}

#[derive(Debug)]
struct PlannedRemovalEntry {
    path: PathBuf,
    kind: PlannedRemovalKind,
}

/// An exact, bounded, no-follow deletion plan for recovery-owned directory trees.
///
/// Execution never enumerates a directory again and never calls recursive removal. A descendant
/// injected after planning therefore makes a final `remove_dir` fail with `DirectoryNotEmpty`
/// instead of expanding the cleanup work beyond the admitted envelope.
#[derive(Debug)]
pub(crate) struct RecursiveNamespaceRemovalPlan {
    entries: Vec<PlannedRemovalEntry>,
    root_count: usize,
}

impl RecursiveNamespaceRemovalPlan {
    pub(crate) fn include_file_like_roots_with_admission<F>(
        mut self,
        roots: &[PathBuf],
        base_retained_bytes: usize,
        mut admit: F,
    ) -> Result<Self>
    where
        F: FnMut(usize) -> Result<()>,
    {
        let retained_paths = self.entries.iter().try_fold(0usize, |total, entry| {
            total.checked_add(entry.path.capacity()).ok_or_else(|| {
                TsinkError::Other(
                    "bounded recovery deletion-plan path accounting overflow".to_string(),
                )
            })
        })?;
        let prospective_capacity =
            self.entries.len().checked_add(roots.len()).ok_or_else(|| {
                TsinkError::Other("bounded recovery deletion-plan length overflow".to_string())
            })?;
        let root_path_bytes = roots.iter().try_fold(0usize, |total, root| {
            total.checked_add(root.capacity()).ok_or_else(|| {
                TsinkError::Other("bounded recovery file-root path accounting overflow".to_string())
            })
        })?;
        let prospective = base_retained_bytes
            .checked_add(
                prospective_capacity
                    .checked_mul(std::mem::size_of::<PlannedRemovalEntry>())
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "bounded recovery deletion-plan capacity overflow".to_string(),
                        )
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(retained_paths))
            .and_then(|bytes| bytes.checked_add(root_path_bytes))
            .ok_or_else(|| {
                TsinkError::Other("bounded recovery deletion-plan memory overflow".to_string())
            })?;
        admit(prospective)?;
        self.entries.try_reserve(roots.len()).map_err(|_| {
            TsinkError::Other(
                "unable to extend bounded recovery namespace deletion plan".to_string(),
            )
        })?;
        for root in roots {
            let metadata =
                std::fs::symlink_metadata(root).map_err(|source| TsinkError::IoWithPath {
                    path: root.clone(),
                    source,
                })?;
            if metadata.file_type().is_dir() && !is_link_or_reparse_point(&metadata) {
                return Err(TsinkError::DataCorruption(format!(
                    "planned recovery file-like root changed into a directory: {}",
                    root.display()
                )));
            }
            self.entries.push(PlannedRemovalEntry {
                path: root.clone(),
                kind: PlannedRemovalKind::FileLike,
            });
        }
        self.root_count = self.root_count.checked_add(roots.len()).ok_or_else(|| {
            TsinkError::Other("bounded recovery root counter overflow".to_string())
        })?;
        let actual = base_retained_bytes
            .checked_add(
                self.entries
                    .capacity()
                    .checked_mul(std::mem::size_of::<PlannedRemovalEntry>())
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "bounded recovery deletion-plan capacity overflow".to_string(),
                        )
                    })?,
            )
            .and_then(|bytes| {
                self.entries
                    .iter()
                    .try_fold(bytes, |total, entry| {
                        total.checked_add(entry.path.capacity()).ok_or(())
                    })
                    .ok()
            })
            .ok_or_else(|| {
                TsinkError::Other("bounded recovery deletion-plan memory overflow".to_string())
            })?;
        admit(actual)?;
        Ok(self)
    }

    pub(crate) fn remove(mut self) -> Result<usize> {
        self.entries.sort_unstable_by(|left, right| {
            right
                .path
                .components()
                .count()
                .cmp(&left.path.components().count())
                .then_with(|| right.path.cmp(&left.path))
        });
        for entry in self.entries {
            let metadata = match std::fs::symlink_metadata(&entry.path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: entry.path,
                        source,
                    });
                }
            };
            let is_plain_directory =
                metadata.file_type().is_dir() && !is_link_or_reparse_point(&metadata);
            match entry.kind {
                PlannedRemovalKind::Directory if !is_plain_directory => {
                    return Err(TsinkError::DataCorruption(format!(
                        "planned recovery directory changed type before removal: {}",
                        entry.path.display()
                    )));
                }
                PlannedRemovalKind::FileLike if is_plain_directory => {
                    return Err(TsinkError::DataCorruption(format!(
                        "planned recovery file-like entry changed into a directory before removal: {}",
                        entry.path.display()
                    )));
                }
                PlannedRemovalKind::Directory => {
                    remove_empty_dir_if_exists(&entry.path).map_err(|source| {
                        TsinkError::IoWithPath {
                            path: entry.path,
                            source,
                        }
                    })?;
                }
                PlannedRemovalKind::FileLike => {
                    remove_file_if_exists(&entry.path).map_err(|source| {
                        TsinkError::IoWithPath {
                            path: entry.path,
                            source,
                        }
                    })?;
                }
            }
        }
        Ok(self.root_count)
    }
}

/// Validates all descendants of recovery-owned directory roots within one global work envelope.
///
/// The roots themselves are not charged because callers have already counted them while scanning
/// their parent namespace. Every descendant is charged, including non-UTF-8 names, links, and
/// special entries. Link-like entries are never traversed. This is a preflight for recursive
/// removal: callers must validate every candidate root in one call before deleting the first one.
/// Builds one exact recursive deletion plan using a shared global namespace counter and a caller
/// supplied memory admission function. Directory enumeration is depth-first and streaming, so
/// unrelated siblings never accumulate in a `Vec<DirEntry>` or pending-path stack.
#[cfg(test)]
fn validate_recursive_namespace_bounded(
    roots: &[PathBuf],
    max_entries: usize,
    max_depth: u32,
    operation: &str,
) -> Result<RecursiveNamespaceRemovalPlan> {
    let mut entry_budget = RecoveryNamespaceBudget::new(max_entries);
    validate_recursive_namespace_with_admission(
        roots,
        &mut entry_budget,
        max_depth,
        operation,
        0,
        |_| Ok(()),
    )
}

pub(crate) fn validate_recursive_namespace_with_admission<F>(
    roots: &[PathBuf],
    entry_budget: &mut RecoveryNamespaceBudget,
    max_depth: u32,
    operation: &str,
    base_retained_bytes: usize,
    mut admit: F,
) -> Result<RecursiveNamespaceRemovalPlan>
where
    F: FnMut(usize) -> Result<()>,
{
    let mut builder = RecursiveNamespacePlanBuilder {
        entries: Vec::new(),
        retained_path_bytes: 0,
        base_retained_bytes,
        operation,
        admit: &mut admit,
    };
    builder.admit_current(0)?;
    for root in roots {
        let metadata =
            std::fs::symlink_metadata(root).map_err(|source| TsinkError::IoWithPath {
                path: root.clone(),
                source,
            })?;
        if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
            return Err(TsinkError::DataCorruption(format!(
                "{operation} root is link-like or not a directory: {}",
                root.display()
            )));
        }
        collect_recursive_namespace_streaming(
            root,
            0,
            0,
            max_depth,
            operation,
            entry_budget,
            &mut builder,
        )?;
        builder.push(root.clone(), PlannedRemovalKind::Directory, 0)?;
    }
    Ok(RecursiveNamespaceRemovalPlan {
        entries: builder.entries,
        root_count: roots.len(),
    })
}

struct RecursiveNamespacePlanBuilder<'a, F> {
    entries: Vec<PlannedRemovalEntry>,
    retained_path_bytes: usize,
    base_retained_bytes: usize,
    operation: &'a str,
    admit: &'a mut F,
}

impl<F> RecursiveNamespacePlanBuilder<'_, F>
where
    F: FnMut(usize) -> Result<()>,
{
    fn required_bytes(&self, traversal_bytes: usize) -> Result<usize> {
        self.base_retained_bytes
            .checked_add(
                self.entries
                    .capacity()
                    .checked_mul(std::mem::size_of::<PlannedRemovalEntry>())
                    .ok_or_else(|| {
                        TsinkError::Other(format!(
                            "{0} deletion-plan vector capacity overflow",
                            self.operation
                        ))
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(self.retained_path_bytes))
            .and_then(|bytes| bytes.checked_add(traversal_bytes))
            .ok_or_else(|| TsinkError::Other(format!("{} memory model overflow", self.operation)))
    }

    fn admit_current(&mut self, traversal_bytes: usize) -> Result<()> {
        let required = self.required_bytes(traversal_bytes)?;
        (self.admit)(required)
    }

    fn push(
        &mut self,
        path: PathBuf,
        kind: PlannedRemovalKind,
        traversal_bytes: usize,
    ) -> Result<()> {
        let path_bytes = path.capacity();
        let prospective_capacity = if self.entries.len() == self.entries.capacity() {
            self.entries.len().checked_add(1).ok_or_else(|| {
                TsinkError::Other(format!("{} plan length overflow", self.operation))
            })?
        } else {
            self.entries.capacity()
        };
        let prospective = self
            .base_retained_bytes
            .checked_add(
                prospective_capacity
                    .checked_mul(std::mem::size_of::<PlannedRemovalEntry>())
                    .ok_or_else(|| {
                        TsinkError::Other(format!("{} plan capacity overflow", self.operation))
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(self.retained_path_bytes))
            .and_then(|bytes| bytes.checked_add(path_bytes))
            .and_then(|bytes| bytes.checked_add(traversal_bytes))
            .ok_or_else(|| {
                TsinkError::Other(format!("{} memory model overflow", self.operation))
            })?;
        (self.admit)(prospective)?;
        if self.entries.len() == self.entries.capacity() {
            self.entries.try_reserve_exact(1).map_err(|_| {
                TsinkError::Other(format!(
                    "unable to allocate recursive namespace deletion plan for {}",
                    self.operation
                ))
            })?;
        }
        self.retained_path_bytes = self
            .retained_path_bytes
            .checked_add(path_bytes)
            .ok_or_else(|| {
                TsinkError::Other(format!("{} retained path overflow", self.operation))
            })?;
        self.entries.push(PlannedRemovalEntry { path, kind });
        self.admit_current(traversal_bytes)
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_recursive_namespace_streaming<F>(
    directory: &Path,
    depth: u32,
    traversal_path_bytes: usize,
    max_depth: u32,
    operation: &str,
    entry_budget: &mut RecoveryNamespaceBudget,
    builder: &mut RecursiveNamespacePlanBuilder<'_, F>,
) -> Result<()>
where
    F: FnMut(usize) -> Result<()>,
{
    if depth > max_depth {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} exceeds its {max_depth}-level recursive depth bound: {}",
            directory.display()
        )));
    }
    let traversal_frames = usize::try_from(depth)
        .unwrap_or(usize::MAX)
        .checked_add(1)
        .and_then(|frames| {
            frames.checked_mul(
                std::mem::size_of::<std::fs::ReadDir>() + 2 * std::mem::size_of::<usize>(),
            )
        })
        .ok_or_else(|| TsinkError::Other(format!("{operation} frame model overflow")))?;
    builder.admit_current(
        traversal_path_bytes
            .checked_add(traversal_frames)
            .ok_or_else(|| TsinkError::Other(format!("{operation} memory model overflow")))?,
    )?;
    let entries = std::fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in entries {
        entry_budget.observe_entry(directory, operation)?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let component_bytes = name.as_encoded_bytes().len();
        let anticipated_path_bytes = directory
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(component_bytes)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| TsinkError::Other(format!("{operation} path size overflow")))?;
        let transient = traversal_path_bytes
            .checked_add(traversal_frames)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::fs::DirEntry>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
            .and_then(|bytes| bytes.checked_add(component_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
            .and_then(|bytes| bytes.checked_add(anticipated_path_bytes))
            .ok_or_else(|| TsinkError::Other(format!("{operation} memory model overflow")))?;
        builder.admit_current(transient)?;
        let path = directory.join(&name);
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })?;
        let is_plain_directory =
            metadata.file_type().is_dir() && !is_link_or_reparse_point(&metadata);
        if is_plain_directory {
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                TsinkError::Other(format!(
                    "{operation} recursive depth counter overflow at {}",
                    path.display()
                ))
            })?;
            if child_depth > max_depth {
                return Err(TsinkError::DataCorruption(format!(
                    "{operation} exceeds its {max_depth}-level recursive depth bound: {}",
                    path.display()
                )));
            }
            let child_traversal = traversal_path_bytes
                .checked_add(std::mem::size_of::<PathBuf>())
                .and_then(|bytes| bytes.checked_add(path.capacity()))
                .ok_or_else(|| TsinkError::Other(format!("{operation} traversal path overflow")))?;
            collect_recursive_namespace_streaming(
                &path,
                child_depth,
                child_traversal,
                max_depth,
                operation,
                entry_budget,
                builder,
            )?;
            builder.push(path, PlannedRemovalKind::Directory, traversal_path_bytes)?;
        } else {
            builder.push(path, PlannedRemovalKind::FileLike, traversal_path_bytes)?;
        }
    }
    Ok(())
}

#[cfg(test)]
type DirectorySyncHook = dyn Fn(&Path) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
type FileSyncHook = dyn Fn(&Path) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
type TmpWriteFailureHook =
    dyn Fn(&Path, &mut std::fs::File, &[u8]) -> Option<TsinkError> + Send + Sync + 'static;

#[cfg(test)]
pub(crate) struct DirectorySyncHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
pub(crate) struct FileSyncHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
pub(crate) struct TmpWriteFailureHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for TmpWriteFailureHookGuard {
    fn drop(&mut self) {
        *tmp_write_failure_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(test)]
impl Drop for DirectorySyncHookGuard {
    fn drop(&mut self) {
        *directory_sync_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(test)]
impl Drop for FileSyncHookGuard {
    fn drop(&mut self) {
        *file_sync_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(test)]
fn directory_sync_hook_slot() -> &'static std::sync::Mutex<Option<std::sync::Arc<DirectorySyncHook>>>
{
    static DIRECTORY_SYNC_HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<std::sync::Arc<DirectorySyncHook>>>,
    > = std::sync::OnceLock::new();
    DIRECTORY_SYNC_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn file_sync_hook_slot() -> &'static std::sync::Mutex<Option<std::sync::Arc<FileSyncHook>>> {
    static FILE_SYNC_HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<std::sync::Arc<FileSyncHook>>>,
    > = std::sync::OnceLock::new();
    FILE_SYNC_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn directory_sync_test_lock() -> &'static std::sync::Mutex<()> {
    static DIRECTORY_SYNC_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    DIRECTORY_SYNC_TEST_LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn file_sync_test_lock() -> &'static std::sync::Mutex<()> {
    static FILE_SYNC_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    FILE_SYNC_TEST_LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn tmp_write_failure_hook_slot(
) -> &'static std::sync::Mutex<Option<std::sync::Arc<TmpWriteFailureHook>>> {
    static TMP_WRITE_FAILURE_HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<std::sync::Arc<TmpWriteFailureHook>>>,
    > = std::sync::OnceLock::new();
    TMP_WRITE_FAILURE_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn tmp_write_failure_test_lock() -> &'static std::sync::Mutex<()> {
    static TMP_WRITE_FAILURE_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    TMP_WRITE_FAILURE_TEST_LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn invoke_directory_sync_hook(path: &Path) -> Result<()> {
    let hook = directory_sync_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(path)?;
    }
    Ok(())
}

#[cfg(test)]
fn invoke_file_sync_hook(path: &Path) -> Result<()> {
    let hook = file_sync_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(path)?;
    }
    Ok(())
}

#[cfg(test)]
fn invoke_tmp_write_failure_hook(
    path: &Path,
    file: &mut std::fs::File,
    bytes: &[u8],
) -> Option<TsinkError> {
    let hook = tmp_write_failure_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    hook.and_then(|hook| hook(path, file, bytes))
}

#[cfg(test)]
pub(crate) fn fail_directory_sync_once(
    path: PathBuf,
    message: impl Into<String>,
) -> DirectorySyncHookGuard {
    fail_directory_sync_matching_once(move |candidate| candidate == path.as_path(), message)
}

/// Installs a one-shot directory-sync failure whose matcher is only invoked beneath `scope`.
///
/// Directory-sync hooks are process-global so they can observe work performed by helper threads.
/// Keeping the scope check outside the caller's matcher prevents observation or mutation side
/// effects from unrelated tests that synchronize directories concurrently.
#[cfg(test)]
pub(crate) fn fail_directory_sync_matching_once_under<F>(
    scope: PathBuf,
    matcher: F,
    message: impl Into<String>,
) -> DirectorySyncHookGuard
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    fail_directory_sync_matching_once(
        move |candidate| candidate.starts_with(scope.as_path()) && matcher(candidate),
        message,
    )
}

#[cfg(test)]
pub(crate) fn fail_directory_sync_matching_once<F>(
    matcher: F,
    message: impl Into<String>,
) -> DirectorySyncHookGuard
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let lock = directory_sync_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let failed = Arc::new(AtomicBool::new(false));
    let message = message.into();
    *directory_sync_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::new(move |candidate| {
        if matcher(candidate) && !failed.swap(true, Ordering::SeqCst) {
            return Err(TsinkError::Other(message.clone()));
        }
        Ok(())
    }));
    DirectorySyncHookGuard { _lock: lock }
}

#[cfg(test)]
pub(crate) fn fail_file_sync_matching_once<F>(
    matcher: F,
    message: impl Into<String>,
) -> FileSyncHookGuard
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let lock = file_sync_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let failed = Arc::new(AtomicBool::new(false));
    let message = message.into();
    *file_sync_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::new(move |candidate| {
        if matcher(candidate) && !failed.swap(true, Ordering::SeqCst) {
            return Err(TsinkError::Other(message.clone()));
        }
        Ok(())
    }));
    FileSyncHookGuard { _lock: lock }
}

#[cfg(test)]
pub(crate) fn fail_tmp_write_after_bytes_once(
    target: PathBuf,
    bytes_before_failure: usize,
    kind: std::io::ErrorKind,
    message: impl Into<String>,
) -> TmpWriteFailureHookGuard {
    use std::sync::atomic::{AtomicBool, Ordering};

    let lock = tmp_write_failure_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let failed = Arc::new(AtomicBool::new(false));
    let message = message.into();
    let target_parent = target.parent().map(Path::to_path_buf);
    let target_prefix = target
        .file_name()
        .map(|name| format!(".{}.tmp-", name.to_string_lossy()));
    *tmp_write_failure_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::new(
        move |candidate: &Path, file: &mut std::fs::File, bytes: &[u8]| {
            let matches_target = candidate.parent().map(Path::to_path_buf) == target_parent
                && candidate
                    .file_name()
                    .map(|name| name.to_string_lossy())
                    .zip(target_prefix.as_deref())
                    .is_some_and(|(name, prefix)| name.starts_with(prefix));
            if !matches_target || failed.swap(true, Ordering::SeqCst) {
                return None;
            }
            let prefix_len = bytes_before_failure.min(bytes.len());
            if let Err(source) = file.write_all(&bytes[..prefix_len]) {
                return Some(TsinkError::Io(source));
            }
            Some(TsinkError::Io(std::io::Error::new(kind, message.clone())))
        },
    ));
    TmpWriteFailureHookGuard { _lock: lock }
}

#[cfg(test)]
pub(crate) fn panic_tmp_write_after_bytes_once(
    target: PathBuf,
    bytes_before_panic: usize,
    message: impl Into<String>,
) -> TmpWriteFailureHookGuard {
    use std::sync::atomic::{AtomicBool, Ordering};

    let lock = tmp_write_failure_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let panicked = Arc::new(AtomicBool::new(false));
    let message = message.into();
    let target_parent = target.parent().map(Path::to_path_buf);
    let target_prefix = target
        .file_name()
        .map(|name| format!(".{}.tmp-", name.to_string_lossy()));
    *tmp_write_failure_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::new(
        move |candidate: &Path, file: &mut std::fs::File, bytes: &[u8]| {
            let matches_target = candidate.parent().map(Path::to_path_buf) == target_parent
                && candidate
                    .file_name()
                    .map(|name| name.to_string_lossy())
                    .zip(target_prefix.as_deref())
                    .is_some_and(|(name, prefix)| name.starts_with(prefix));
            if !matches_target || panicked.swap(true, Ordering::SeqCst) {
                return None;
            }
            let prefix_len = bytes_before_panic.min(bytes.len());
            file.write_all(&bytes[..prefix_len])
                .expect("injected temporary-write panic prefix must be writable");
            panic!("{message}");
        },
    ));
    TmpWriteFailureHookGuard { _lock: lock }
}

pub(crate) fn path_exists_no_follow(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn is_link_or_reparse_point(metadata: &std::fs::Metadata) -> bool {
    let is_symlink = metadata.file_type().is_symlink();
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        is_symlink || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        is_symlink
    }
}

/// Returns whether two paths overlap after resolving every existing component.
///
/// Missing suffixes are retained lexically after their nearest existing ancestor. This catches
/// aliases through intermediate symlinks while still supporting a restore target that does not
/// exist yet.
pub(crate) fn paths_overlap_resolved(left: &Path, right: &Path) -> Result<bool> {
    let left = resolve_path_allow_missing(left)?;
    let right = resolve_path_allow_missing(right)?;
    Ok(left.starts_with(&right) || right.starts_with(&left))
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
                if let Ok(canonical) = std::fs::canonicalize(&resolved) {
                    resolved = canonical;
                }
            }
            Component::Normal(name) => {
                resolved.push(name);
                match std::fs::canonicalize(&resolved) {
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

#[cfg(windows)]
fn is_transient_windows_fs_error(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(5 | 32 | 33))
        || err.kind() == std::io::ErrorKind::PermissionDenied
}

#[cfg(windows)]
fn retry_windows_fs_operation<T>(
    mut operation: impl FnMut() -> std::io::Result<T>,
) -> std::io::Result<T> {
    const RETRY_ATTEMPTS: usize = 128;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(10);

    for attempt in 0..RETRY_ATTEMPTS {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) if attempt + 1 < RETRY_ATTEMPTS && is_transient_windows_fs_error(&err) => {
                std::thread::sleep(RETRY_DELAY);
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("Windows filesystem retry loop should return from inside the loop")
}

#[cfg(windows)]
fn remove_dir_all_with_retry(path: &Path) -> std::io::Result<()> {
    retry_windows_fs_operation(|| std::fs::remove_dir_all(path))
}

#[cfg(not(windows))]
fn remove_dir_all_with_retry(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir_all(path)
}

#[cfg(windows)]
fn remove_empty_dir_with_retry(path: &Path) -> std::io::Result<()> {
    retry_windows_fs_operation(|| std::fs::remove_dir(path))
}

#[cfg(not(windows))]
fn remove_empty_dir_with_retry(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir(path)
}

#[cfg(windows)]
fn remove_file_with_retry(path: &Path) -> std::io::Result<()> {
    retry_windows_fs_operation(|| std::fs::remove_file(path))
}

#[cfg(not(windows))]
fn remove_file_with_retry(path: &Path) -> std::io::Result<()> {
    std::fs::remove_file(path)
}

#[cfg(windows)]
fn rename_path_with_retry(source: &Path, destination: &Path) -> std::io::Result<()> {
    retry_windows_fs_operation(|| std::fs::rename(source, destination))
}

#[cfg(not(windows))]
fn rename_path_with_retry(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

/// Renames one path only if `destination` is still absent, without synchronizing parents.
///
/// This is for transactions that must distinguish publication from a later parent-sync failure
/// and perform their own rollback. Most callers should use
/// [`rename_noreplace_and_sync_parents`].
pub(crate) fn rename_path_noreplace(source: &Path, destination: &Path) -> Result<()> {
    rename_path_noreplace_with_retry(source, destination).map_err(|source_err| {
        TsinkError::IoWithPath {
            path: destination.to_path_buf(),
            source: source_err,
        }
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_path_noreplace_with_retry(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rename source contains an interior NUL byte",
        )
    })?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rename destination contains an interior NUL byte",
        )
    })?;
    // Invoke the kernel directly rather than importing glibc's newer `renameat2` symbol, which
    // would unnecessarily raise the minimum glibc required by otherwise-compatible binaries.
    let renamed = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if renamed == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_vendor = "apple")]
fn rename_path_noreplace_with_retry(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rename source contains an interior NUL byte",
        )
    })?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rename destination contains an interior NUL byte",
        )
    })?;
    let renamed =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if renamed == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn rename_path_noreplace_with_retry(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "MoveFileExW"]
        fn move_file_ex_w(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    fn wide_path(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path contains an interior NUL byte: {}", path.display()),
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    let source = wide_path(source)?;
    let destination = wide_path(destination)?;
    retry_windows_fs_operation(|| {
        let moved = unsafe {
            move_file_ex_w(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    })
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android")),
    not(target_vendor = "apple")
))]
fn rename_path_noreplace_with_retry(_source: &Path, _destination: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "this platform has no configured atomic no-replace directory rename primitive",
    ))
}

pub(crate) fn remove_dir_if_exists(path: &Path) -> std::io::Result<bool> {
    match remove_dir_all_with_retry(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn remove_empty_dir_if_exists(path: &Path) -> std::io::Result<bool> {
    match remove_empty_dir_with_retry(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn remove_file_if_exists(path: &Path) -> std::io::Result<bool> {
    match remove_file_with_retry(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn remove_path_if_exists(path: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    if metadata.is_dir() {
        let _ = remove_dir_if_exists(path)?;
    } else {
        let _ = remove_file_if_exists(path)?;
    }

    Ok(())
}

pub(crate) fn remove_path_if_exists_and_sync_parent(path: &Path) -> Result<()> {
    let existed = path_exists_no_follow(path)?;
    remove_path_if_exists(path)?;
    if existed {
        sync_parent_dir(path)?;
    }
    Ok(())
}

/// Captures the stable filesystem identity of one plain directory.
///
/// Callers use this before a create/rename publication so later error cleanup can distinguish the
/// owned directory from an unrelated entry installed at the same pathname.
#[cfg(test)]
pub(crate) fn capture_plain_directory_identity(
    path: &Path,
    operation: &str,
) -> Result<same_file::Handle> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} root is link-like or not a directory: {}",
            path.display()
        )));
    }
    let identity = same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if !path_matches_plain_directory_identity(path, &identity)? {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} root identity changed while it was captured: {}",
            path.display()
        )));
    }
    Ok(identity)
}

/// Returns whether `path` is still the same plain directory as `expected`.
#[cfg(test)]
pub(crate) fn path_matches_plain_directory_identity(
    path: &Path,
    expected: &same_file::Handle,
) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Ok(false);
    }
    let current = same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(&current == expected)
}

/// Removes one owned directory tree through an exact, bounded, no-follow deletion plan.
///
/// Every descendant is admitted before the first unlink. Execution then removes only the planned
/// entries, deepest first, without enumerating again; a late descendant makes the final directory
/// removal fail instead of expanding work or deleting the injected entry.
#[cfg(test)]
pub(crate) fn remove_owned_directory_tree_bounded_and_sync_parent(
    path: &Path,
    max_entries: usize,
    max_depth: u32,
    operation: &str,
) -> Result<bool> {
    remove_owned_directory_tree_bounded_and_sync_parent_inner(
        path,
        None,
        max_entries,
        max_depth,
        operation,
    )
}

/// Removes one owned directory tree through identity-checked bounded cleanup.
#[cfg(test)]
pub(crate) fn remove_owned_directory_tree_bounded_and_sync_parent_with_identity(
    path: &Path,
    expected: &same_file::Handle,
    max_entries: usize,
    max_depth: u32,
    operation: &str,
) -> Result<bool> {
    remove_owned_directory_tree_bounded_and_sync_parent_inner(
        path,
        Some(expected),
        max_entries,
        max_depth,
        operation,
    )
}

#[cfg(test)]
fn remove_owned_directory_tree_bounded_and_sync_parent_inner(
    path: &Path,
    expected: Option<&same_file::Handle>,
    max_entries: usize,
    max_depth: u32,
    operation: &str,
) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} owned root changed into a link-like or non-directory entry: {}",
            path.display()
        )));
    }
    if let Some(identity) = expected {
        if !path_matches_plain_directory_identity(path, identity)? {
            return Err(TsinkError::DataCorruption(format!(
                "refusing {operation} because the owned root identity changed: {}",
                path.display()
            )));
        }
    }

    let mut entry_budget = RecoveryNamespaceBudget::new(max_entries);
    let plan = validate_recursive_namespace_with_admission(
        &[path.to_path_buf()],
        &mut entry_budget,
        max_depth,
        operation,
        0,
        |_| Ok(()),
    )?;
    if let Some(identity) = expected {
        if !path_matches_plain_directory_identity(path, identity)? {
            return Err(TsinkError::DataCorruption(format!(
                "refusing {operation} because the owned root identity changed after cleanup planning: {}",
                path.display()
            )));
        }
    }
    plan.remove()?;
    sync_parent_dir(path)?;
    Ok(true)
}

/// Removes an owned path and credits the bytes that actually disappeared from a managed budget.
///
/// A zero-byte recovery reservation keeps a concurrent full-tree reconciliation from racing the
/// deletion. Accounting remains conservatively unchanged until an exclusive reconciliation can
/// scan the resulting tree, including any externally-added files.
pub(crate) fn remove_path_if_exists_and_sync_parent_budgeted(
    path: &Path,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
) -> Result<()> {
    remove_path_if_exists_and_sync_parent_budgeted_with_reconciliation_memory_limit(
        path,
        budget,
        category,
        usize::MAX,
    )
}

pub(crate) fn remove_path_if_exists_and_sync_parent_budgeted_with_reconciliation_memory_limit(
    path: &Path,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
    reconciliation_memory_limit: usize,
) -> Result<()> {
    let Some(budget) = budget else {
        return remove_path_if_exists_and_sync_parent(path);
    };
    if !budget.governs_entry(path)? {
        return remove_path_if_exists_and_sync_parent(path);
    }

    let reservation = budget.reserve(category, 0, crate::DiskReservationKind::Recovery)?;
    let removal_result = remove_path_if_exists_and_sync_parent(path);
    let settlement_result = reservation.commit(0, 0);
    let reconciliation_result = budget
        .reconcile_when_idle_with_memory_limit(reconciliation_memory_limit)
        .map(|_| ());

    let mut errors = Vec::new();
    if let Err(err) = &removal_result {
        errors.push(format!("remove failed: {err}"));
    }
    if let Err(err) = &settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = &reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    if errors.is_empty() {
        Ok(())
    } else if errors.len() == 1 {
        match (removal_result, settlement_result, reconciliation_result) {
            (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
            _ => unreachable!("one recorded error must have a matching failed result"),
        }
    } else {
        Err(TsinkError::Other(format!(
            "budgeted removal of {} failed: {}",
            path.display(),
            errors.join("; ")
        )))
    }
}

fn next_stage_dir_candidate(target: &Path, purpose: &str) -> Result<PathBuf> {
    let Some(parent) = target.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{purpose} target has no parent directory: {}",
            target.display()
        )));
    };

    let target_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("snapshot");

    let nonce = STAGE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(".tmp-tsink-{purpose}-{target_name}-{nonce:016x}")))
}

pub(crate) fn stage_dir_path(target: &Path, purpose: &str) -> Result<PathBuf> {
    for _ in 0..256 {
        let candidate = next_stage_dir_candidate(target, purpose)?;
        if !path_exists_no_follow(&candidate)? {
            return Ok(candidate);
        }
    }

    Err(TsinkError::Other(format!(
        "failed to allocate unique staging path for {}",
        target.display()
    )))
}

/// Atomically allocates and creates an absent staging directory beside `target`.
///
/// A separate `stage_dir_path` followed by `create_dir_all` is a check-then-create race:
/// another process can install a directory or link at the selected path and cause a caller to
/// populate or later clean up an entry it did not create. This helper uses the operating system's
/// exclusive single-directory create operation and retries only when that exact candidate already
/// exists. The target parent must already exist.
#[cfg(test)]
pub(crate) fn create_unique_staging_dir(target: &Path, purpose: &str) -> Result<PathBuf> {
    for _ in 0..256 {
        let candidate = next_stage_dir_candidate(target, purpose)?;
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: candidate,
                    source,
                });
            }
        }
    }

    Err(TsinkError::Other(format!(
        "failed to atomically create a unique staging directory for {}",
        target.display()
    )))
}

/// Creates one already-planned staging directory with create-exclusive semantics.
///
/// This does not choose another name when the planned entry exists. It is intended for coordinated
/// replacement transactions that admitted and recorded the exact staging path before entering
/// their mutation closure.
pub(crate) fn create_staging_dir_exclusive(path: &Path) -> Result<()> {
    std::fs::create_dir(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
pub(crate) fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    let measurement = measure_restore_directory(source)?;
    copy_dir_contents_bounded(source, destination, measurement)
}

#[cfg(test)]
pub(crate) fn copy_dir_if_exists(source: &Path, destination: &Path) -> Result<()> {
    match std::fs::symlink_metadata(source) {
        Ok(metadata) => {
            if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "snapshot source is not a directory: {}",
                    source.display()
                )));
            }
            copy_dir_recursive(source, destination)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestoreDirectoryMeasurement {
    pub(crate) logical_bytes: u64,
    pub(crate) entry_count: u64,
    pub(crate) max_directory_depth: u32,
}

impl RestoreDirectoryMeasurement {
    pub(crate) fn staging_admission_bytes(self, entry_allowance_bytes: u64) -> Result<u64> {
        let entry_allowance = self
            .entry_count
            .checked_mul(entry_allowance_bytes)
            .ok_or_else(|| {
                TsinkError::Other(
                    "snapshot restore entry staging allowance exceeds the supported byte range"
                        .to_string(),
                )
            })?;
        self.logical_bytes
            .checked_add(entry_allowance)
            .ok_or_else(|| {
                TsinkError::Other(
                    "snapshot restore staging admission exceeds the supported byte range"
                        .to_string(),
                )
            })
    }
}

/// Measures the finite logical-byte and entry-count bounds for a trusted restore tree.
///
/// The root directory counts as one entry. Static symlink, Windows reparse-point, and special-entry
/// checks run before destination mutation, but the caller must keep the source immutable because
/// portable path traversal cannot prevent a concurrent namespace swap after this check.
pub(crate) fn measure_restore_directory(source: &Path) -> Result<RestoreDirectoryMeasurement> {
    let metadata =
        std::fs::symlink_metadata(source).map_err(|source_err| TsinkError::IoWithPath {
            path: source.to_path_buf(),
            source: source_err,
        })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a plain directory: {}",
            source.display()
        )));
    }

    let mut measurement = RestoreDirectoryMeasurement {
        logical_bytes: 0,
        entry_count: 1,
        max_directory_depth: 0,
    };
    let mut pending = vec![(source.to_path_buf(), 0u32)];
    while let Some((directory, directory_depth)) = pending.pop() {
        for entry in std::fs::read_dir(&directory).map_err(|source_err| TsinkError::IoWithPath {
            path: directory.clone(),
            source: source_err,
        })? {
            let entry = entry.map_err(|source_err| TsinkError::IoWithPath {
                path: directory.clone(),
                source: source_err,
            })?;
            let path = entry.path();
            let metadata =
                std::fs::symlink_metadata(&path).map_err(|source_err| TsinkError::IoWithPath {
                    path: path.clone(),
                    source: source_err,
                })?;
            if is_link_or_reparse_point(&metadata) {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "unsupported link-like entry while measuring snapshot: {}",
                    path.display()
                )));
            }
            measurement.entry_count = measurement.entry_count.checked_add(1).ok_or_else(|| {
                TsinkError::Other(
                    "snapshot restore entry count exceeds the supported range".to_string(),
                )
            })?;
            if measurement.entry_count > crate::MAX_SNAPSHOT_RESTORE_ENTRIES {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "snapshot restore entry count {} exceeds limit {} at {}",
                    measurement.entry_count,
                    crate::MAX_SNAPSHOT_RESTORE_ENTRIES,
                    path.display()
                )));
            }
            let file_type = metadata.file_type();
            if file_type.is_dir() {
                let child_depth = directory_depth.checked_add(1).ok_or_else(|| {
                    TsinkError::Other(
                        "snapshot restore directory depth exceeds the supported range".to_string(),
                    )
                })?;
                if child_depth > crate::MAX_SNAPSHOT_RESTORE_DEPTH {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "snapshot restore directory depth {child_depth} exceeds limit {} at {}",
                        crate::MAX_SNAPSHOT_RESTORE_DEPTH,
                        path.display()
                    )));
                }
                measurement.max_directory_depth = measurement.max_directory_depth.max(child_depth);
                pending.push((path, child_depth));
            } else if file_type.is_file() {
                measurement.logical_bytes = measurement
                    .logical_bytes
                    .checked_add(metadata.len())
                    .ok_or_else(|| {
                    TsinkError::Other(format!(
                        "snapshot byte count exceeds the supported range while measuring {}",
                        path.display()
                    ))
                })?;
            } else {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "unsupported non-file entry while measuring snapshot: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(measurement)
}

/// Copies a trusted directory tree within already-admitted logical-byte and entry-count ceilings.
///
/// Every directory and regular file consumes exactly one measured entry. A regular file is copied
/// through a length-limited reader, so source growth can fail the operation but cannot write beyond
/// the measured logical-byte ceiling. Static link-like checks are repeated, but callers must still
/// keep the source immutable to exclude namespace-swap races.
pub(crate) fn copy_dir_contents_bounded(
    source: &Path,
    destination: &Path,
    expected: RestoreDirectoryMeasurement,
) -> Result<()> {
    let mut remaining_bytes = expected.logical_bytes;
    let mut remaining_entries = expected.entry_count;
    let mut observed_max_directory_depth = 0u32;
    copy_dir_contents_bounded_inner(
        source,
        destination,
        &mut remaining_bytes,
        &mut remaining_entries,
        0,
        expected.max_directory_depth,
        &mut observed_max_directory_depth,
    )?;
    if remaining_bytes != 0
        || remaining_entries != 0
        || observed_max_directory_depth != expected.max_directory_depth
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot changed after admission: remaining_bytes={remaining_bytes}, remaining_entries={remaining_entries}, measured_max_depth={}, observed_max_depth={observed_max_directory_depth}",
            expected.max_directory_depth
        )));
    }
    Ok(())
}

fn copy_dir_contents_bounded_inner(
    source: &Path,
    destination: &Path,
    remaining_bytes: &mut u64,
    remaining_entries: &mut u64,
    directory_depth: u32,
    admitted_max_directory_depth: u32,
    observed_max_directory_depth: &mut u32,
) -> Result<()> {
    if directory_depth > crate::MAX_SNAPSHOT_RESTORE_DEPTH
        || directory_depth > admitted_max_directory_depth
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot directory depth {directory_depth} exceeds its admitted bound {admitted_max_directory_depth} at {}",
            source.display()
        )));
    }
    *observed_max_directory_depth = (*observed_max_directory_depth).max(directory_depth);
    let metadata =
        std::fs::symlink_metadata(source).map_err(|source_err| TsinkError::IoWithPath {
            path: source.to_path_buf(),
            source: source_err,
        })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot directory changed into a link-like or non-directory entry: {}",
            source.display()
        )));
    }
    consume_restore_entry(remaining_entries, source)?;
    std::fs::create_dir_all(destination).map_err(|source_err| TsinkError::IoWithPath {
        path: destination.to_path_buf(),
        source: source_err,
    })?;

    for entry in std::fs::read_dir(source).map_err(|source_err| TsinkError::IoWithPath {
        path: source.to_path_buf(),
        source: source_err,
    })? {
        let entry = entry.map_err(|source_err| TsinkError::IoWithPath {
            path: source.to_path_buf(),
            source: source_err,
        })?;
        let entry_source = entry.path();
        let entry_destination = destination.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&entry_source).map_err(|source_err| {
            TsinkError::IoWithPath {
                path: entry_source.clone(),
                source: source_err,
            }
        })?;
        if is_link_or_reparse_point(&metadata) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "unsupported link-like entry while restoring snapshot: {}",
                entry_source.display()
            )));
        }
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            let child_depth = directory_depth.checked_add(1).ok_or_else(|| {
                TsinkError::Other(
                    "snapshot restore directory depth exceeds the supported range".to_string(),
                )
            })?;
            copy_dir_contents_bounded_inner(
                &entry_source,
                &entry_destination,
                remaining_bytes,
                remaining_entries,
                child_depth,
                admitted_max_directory_depth,
                observed_max_directory_depth,
            )?;
            continue;
        }
        if !file_type.is_file() {
            return Err(TsinkError::InvalidConfiguration(format!(
                "unsupported non-file entry while restoring snapshot: {}",
                entry_source.display()
            )));
        }
        consume_restore_entry(remaining_entries, &entry_source)?;

        let admitted_file_bytes = metadata.len();
        if admitted_file_bytes > *remaining_bytes {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot changed after admission: {} requires {admitted_file_bytes} bytes but only {} measured bytes remain",
                entry_source.display(),
                *remaining_bytes
            )));
        }

        let mut source_file =
            open_snapshot_source_regular_file(&entry_source, admitted_file_bytes)?;

        let mut destination_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&entry_destination)
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry_destination.clone(),
                source: source_err,
            })?;
        let copied = {
            let mut bounded_reader = (&mut source_file).take(admitted_file_bytes);
            std::io::copy(&mut bounded_reader, &mut destination_file).map_err(|source_err| {
                TsinkError::IoWithPath {
                    path: entry_destination.clone(),
                    source: source_err,
                }
            })?
        };
        if copied != admitted_file_bytes {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot file changed after admission: {} yielded {copied} of {admitted_file_bytes} bytes",
                entry_source.display()
            )));
        }
        let mut extra = [0u8; 1];
        if source_file
            .read(&mut extra)
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry_source.clone(),
                source: source_err,
            })?
            != 0
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot file grew after admission: {}",
                entry_source.display()
            )));
        }
        validate_opened_snapshot_source_file_identity(
            source_file,
            &entry_source,
            admitted_file_bytes,
        )?;
        destination_file
            .set_permissions(metadata.permissions())
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry_destination.clone(),
                source: source_err,
            })?;
        destination_file
            .flush()
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry_destination.clone(),
                source: source_err,
            })?;
        #[cfg(test)]
        invoke_file_sync_hook(&entry_destination)?;
        destination_file
            .sync_all()
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry_destination.clone(),
                source: source_err,
            })?;
        *remaining_bytes = remaining_bytes.checked_sub(copied).ok_or_else(|| {
            TsinkError::Other("bounded snapshot copy byte accounting underflow".to_string())
        })?;
    }

    sync_dir(destination)
}

fn consume_restore_entry(remaining_entries: &mut u64, path: &Path) -> Result<()> {
    *remaining_entries = remaining_entries.checked_sub(1).ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "snapshot added an entry after admission: {}",
            path.display()
        ))
    })?;
    Ok(())
}

fn open_snapshot_source_regular_file(path: &Path, expected_len: u64) -> Result<std::fs::File> {
    let mut options = OpenOptions::new();
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
    let file = options
        .open(path)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    let metadata = file
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    if is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() != expected_len
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot source changed type or length while opening: {}",
            path.display()
        )));
    }
    Ok(file)
}

fn validate_opened_snapshot_source_file_identity(
    file: std::fs::File,
    path: &Path,
    expected_len: u64,
) -> Result<()> {
    let opened_identity =
        same_file::Handle::from_file(file).map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    let current_metadata =
        std::fs::symlink_metadata(path).map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    if is_link_or_reparse_point(&current_metadata)
        || !current_metadata.file_type().is_file()
        || current_metadata.len() != expected_len
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot source changed type or length while validating: {}",
            path.display()
        )));
    }
    let current_identity =
        same_file::Handle::from_path(path).map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    if opened_identity != current_identity {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot source path changed while copying: {}",
            path.display()
        )));
    }
    Ok(())
}

pub(crate) fn tmp_path_for(path: &Path) -> Result<PathBuf> {
    let Some(parent) = path.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "temporary file target has no parent directory: {}",
            path.display()
        )));
    };

    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    let pid = std::process::id();

    let nonce = STAGE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(".{file_name}.tmp-{pid}-{nonce:016x}")))
}

fn cleanup_failed_tmp_write(tmp_path: &Path, write_err: TsinkError) -> TsinkError {
    match remove_file_if_exists(tmp_path) {
        Ok(_) => write_err,
        Err(cleanup_err) => TsinkError::Other(format!(
            "temporary file write failed: {write_err}; cleanup of {} failed: {cleanup_err}",
            tmp_path.display()
        )),
    }
}

struct ExactLengthWriter<'a, W> {
    inner: &'a mut W,
    remaining: u64,
}

impl<W: Write> Write for ExactLengthWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let requested = u64::try_from(bytes.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "streamed atomic-write chunk exceeds the supported byte range",
            )
        })?;
        if requested > self.remaining {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "streamed atomic write exceeded its declared length by {} bytes",
                    requested - self.remaining
                ),
            ));
        }
        let written = self.inner.write(bytes)?;
        self.remaining = self.remaining.checked_sub(written as u64).ok_or_else(|| {
            std::io::Error::other("streamed atomic-write length accounting underflow")
        })?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) fn write_tmp_and_sync(path: &Path, bytes: &[u8]) -> Result<PathBuf> {
    write_tmp_and_sync_with_observer(path, bytes, |_| {})
}

/// Writes a synchronized atomic-replacement temporary while transferring cleanup ownership as
/// soon as the directory entry exists.
///
/// `on_created` runs immediately after `create_new` succeeds and before any payload write or test
/// hook. Grouped publishers use it to register the generated path in an unwind guard, so a panic
/// during staging cannot strand a temporary whose name the caller never observed.
pub(crate) fn write_tmp_and_sync_with_observer<F>(
    path: &Path,
    bytes: &[u8],
    on_created: F,
) -> Result<PathBuf>
where
    F: FnOnce(&Path),
{
    let Some(parent) = path.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "temporary file target has no parent directory: {}",
            path.display()
        )));
    };
    std::fs::create_dir_all(parent)?;
    let mut on_created = Some(on_created);

    for _ in 0..256 {
        let tmp_path = tmp_path_for(path)?;
        let file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        };
        on_created
            .take()
            .expect("temporary creation observer may only run once")(&tmp_path);

        #[cfg(test)]
        let mut file = file;
        #[cfg(test)]
        if let Some(write_err) = invoke_tmp_write_failure_hook(&tmp_path, &mut file, bytes) {
            drop(file);
            return Err(cleanup_failed_tmp_write(&tmp_path, write_err));
        }

        let mut writer = BufWriter::new(file);
        let write_result = (|| -> Result<()> {
            writer.write_all(bytes)?;
            writer.flush()?;
            #[cfg(test)]
            invoke_file_sync_hook(&tmp_path)?;
            writer.get_ref().sync_all()?;
            Ok(())
        })();
        drop(writer);
        if let Err(write_err) = write_result {
            return Err(cleanup_failed_tmp_write(&tmp_path, write_err));
        }
        return Ok(tmp_path);
    }

    Err(TsinkError::Other(format!(
        "failed to reserve unique temporary file for {}",
        path.display()
    )))
}

/// Atomically replaces a file from an exact-length bounded stream and synchronizes its parent.
///
/// The callback cannot write more than `expected_bytes`, and writing fewer bytes is rejected. This
/// keeps large cleanup rewrites bounded by the callback's own per-record buffer rather than the
/// complete replacement size. As with [`write_file_atomically_and_sync_parent`], an error from the
/// final parent-directory synchronization can be returned after the replacement became visible.
pub fn write_file_atomically_and_sync_parent_with<F>(
    path: &Path,
    expected_bytes: u64,
    write_replacement: F,
) -> Result<()>
where
    F: FnOnce(&mut dyn Write) -> Result<()>,
{
    let parent = path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "streamed atomic-write target has no parent directory: {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)?;

    let (tmp_path, file) = {
        let mut created = None;
        for _ in 0..256 {
            let tmp_path = tmp_path_for(path)?;
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
            {
                Ok(file) => {
                    created = Some((tmp_path, file));
                    break;
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        }
        created.ok_or_else(|| {
            TsinkError::Other(format!(
                "failed to reserve unique streamed replacement for {}",
                path.display()
            ))
        })?
    };

    let mut writer = BufWriter::new(file);
    let write_result = (|| -> Result<()> {
        {
            let mut exact_writer = ExactLengthWriter {
                inner: &mut writer,
                remaining: expected_bytes,
            };
            write_replacement(&mut exact_writer)?;
            if exact_writer.remaining != 0 {
                return Err(TsinkError::Other(format!(
                    "streamed atomic write for {} produced {} fewer bytes than declared",
                    path.display(),
                    exact_writer.remaining
                )));
            }
            exact_writer.flush()?;
        }
        #[cfg(test)]
        invoke_file_sync_hook(&tmp_path)?;
        writer.get_ref().sync_all()?;
        Ok(())
    })();
    drop(writer);
    if let Err(write_err) = write_result {
        return Err(cleanup_failed_tmp_write(&tmp_path, write_err));
    }

    if let Err(rename_err) = rename_tmp(&tmp_path, path) {
        return Err(cleanup_failed_tmp_write(&tmp_path, rename_err));
    }
    sync_parent_dir(path)
}

pub(crate) fn rename_tmp(tmp_path: &Path, path: &Path) -> Result<()> {
    rename_tmp_impl(tmp_path, path)?;
    Ok(())
}

#[cfg(not(windows))]
fn rename_tmp_impl(tmp_path: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::rename(tmp_path, path)
}

#[cfg(windows)]
fn rename_tmp_impl(tmp_path: &Path, path: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "MoveFileExW"]
        fn move_file_ex_w(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    fn wide_path(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path contains an interior NUL byte: {}", path.display()),
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    let source = wide_path(tmp_path)?;
    let destination = wide_path(path)?;
    let flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;
    retry_windows_fs_operation(|| {
        let moved = unsafe { move_file_ex_w(source.as_ptr(), destination.as_ptr(), flags) };
        if moved == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    })
}

pub(crate) fn rename_and_sync_parents(source: &Path, destination: &Path) -> Result<()> {
    rename_path_with_retry(source, destination)?;

    let source_parent = source.parent();
    let destination_parent = destination.parent();
    match (source_parent, destination_parent) {
        (Some(source_parent), Some(destination_parent)) if source_parent == destination_parent => {
            sync_dir(destination_parent)?
        }
        (Some(source_parent), Some(destination_parent)) => {
            sync_dir(source_parent)?;
            sync_dir(destination_parent)?;
        }
        (None, Some(destination_parent)) => sync_dir(destination_parent)?,
        (Some(source_parent), None) => sync_dir(source_parent)?,
        (None, None) => {}
    }

    Ok(())
}

/// Atomically renames `source` only if `destination` is still absent, then synchronizes parents.
///
/// This is the publication primitive for caller-selected snapshot and salvage destinations. It
/// prevents a path created after preflight from being overwritten. Linux/Android use
/// `renameat2(RENAME_NOREPLACE)`, Apple platforms use `renamex_np(RENAME_EXCL)`, and Windows uses
/// `MoveFileExW` without `MOVEFILE_REPLACE_EXISTING`. Other Unix targets fail explicitly instead
/// of falling back to a racy check-then-rename sequence.
pub(crate) fn rename_noreplace_and_sync_parents(source: &Path, destination: &Path) -> Result<()> {
    rename_path_noreplace(source, destination)?;

    let source_parent = source.parent();
    let destination_parent = destination.parent();
    match (source_parent, destination_parent) {
        (Some(source_parent), Some(destination_parent)) if source_parent == destination_parent => {
            sync_dir(destination_parent)?
        }
        (Some(source_parent), Some(destination_parent)) => {
            sync_dir(source_parent)?;
            sync_dir(destination_parent)?;
        }
        (None, Some(destination_parent)) => sync_dir(destination_parent)?,
        (Some(source_parent), None) => sync_dir(source_parent)?,
        (None, None) => {}
    }

    Ok(())
}

pub(crate) fn rename_and_sync_parents_budgeted_reclassify(
    source: &Path,
    destination: &Path,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    from: crate::DiskCategory,
    _to: crate::DiskCategory,
) -> Result<()> {
    let Some(budget) = budget else {
        return rename_and_sync_parents(source, destination);
    };
    if !budget.governs_entry(source)? || !budget.governs_entry(destination)? {
        return rename_and_sync_parents(source, destination);
    }

    let reservation = budget.reserve(from, 0, crate::DiskReservationKind::Recovery)?;
    let rename_result = rename_and_sync_parents(source, destination);
    let rollback_result = if rename_result.is_err() {
        rollback_rename_after_error(source, destination)
    } else {
        Ok(())
    };
    let settlement_result = reservation.commit(0, 0);
    let reconciliation_result = budget.reconcile_when_idle().map(|_| ());
    let post_rename_failure =
        rename_result.is_ok() && (settlement_result.is_err() || reconciliation_result.is_err());
    let post_rename_rollback_result = if post_rename_failure {
        rollback_rename_after_error(source, destination)
    } else {
        Ok(())
    };
    let post_rollback_reconciliation_result = if post_rename_failure {
        budget.reconcile_when_idle().map(|_| ())
    } else {
        Ok(())
    };

    match (
        rename_result,
        rollback_result,
        settlement_result,
        reconciliation_result,
        post_rename_rollback_result,
        post_rollback_reconciliation_result,
    ) {
        (Ok(()), Ok(()), Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(()), Ok(()), Ok(()), Ok(()), Ok(())) => Err(err),
        (
            rename_result,
            rollback_result,
            settlement_result,
            reconciliation_result,
            post_rename_rollback_result,
            post_rollback_reconciliation_result,
        ) => {
            let mut errors = Vec::new();
            if let Err(err) = rename_result {
                errors.push(format!("rename failed: {err}"));
            }
            if let Err(err) = rollback_result {
                errors.push(format!("rename rollback failed: {err}"));
            }
            if let Err(err) = settlement_result {
                errors.push(format!("disk settlement failed: {err}"));
            }
            if let Err(err) = reconciliation_result {
                errors.push(format!("disk reconciliation failed: {err}"));
            }
            if let Err(err) = post_rename_rollback_result {
                errors.push(format!("post-rename rollback failed: {err}"));
            }
            if let Err(err) = post_rollback_reconciliation_result {
                errors.push(format!("post-rollback disk reconciliation failed: {err}"));
            }
            Err(TsinkError::Other(format!(
                "budgeted rename from {} to {} failed: {}",
                source.display(),
                destination.display(),
                errors.join("; ")
            )))
        }
    }
}

fn rollback_rename_after_error(source: &Path, destination: &Path) -> Result<()> {
    let source_exists = path_exists_no_follow(source)?;
    let destination_exists = path_exists_no_follow(destination)?;
    match (source_exists, destination_exists) {
        (false, true) => rename_and_sync_parents(destination, source),
        (true, _) => Ok(()),
        (false, false) => Err(TsinkError::Other(format!(
            "rename failed and neither source {} nor destination {} exists",
            source.display(),
            destination.display()
        ))),
    }
}

#[cfg(not(windows))]
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    let dir = std::fs::File::open(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    #[cfg(test)]
    invoke_directory_sync_hook(path)?;
    dir.sync_all().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(windows)]
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(test)]
    invoke_directory_sync_hook(path)?;
    // Windows does not support flushing directory handles directly.
    let _ = path;
    Ok(())
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// Creates an unmanaged directory tree and makes every parent link crash-durable.
///
/// Unlike the local-disk-budget directory helper, this deliberately performs no quota accounting.
/// It is used for filesystem-backed tier roots outside the managed local data path. Every ancestor
/// is synchronized on every call so a retry after a prior sync failure cannot mistake
/// merely-visible directory entries for durable ones.
pub(crate) fn create_dir_all_and_sync_parents(directory: &Path) -> Result<()> {
    let directory = if directory.is_absolute() {
        directory.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(TsinkError::Io)?
            .join(directory)
    };
    std::fs::create_dir_all(&directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.clone(),
        source,
    })?;

    let mut ancestors = directory.ancestors().collect::<Vec<_>>();
    ancestors.reverse();
    for ancestor in ancestors {
        let metadata = std::fs::metadata(ancestor).map_err(|source| TsinkError::IoWithPath {
            path: ancestor.to_path_buf(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(TsinkError::InvalidConfiguration(format!(
                "directory path contains a non-directory entry: {}",
                ancestor.display()
            )));
        }
        sync_parent_dir(ancestor)?;
    }
    Ok(())
}

pub fn write_file_atomically_and_sync_parent(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = write_tmp_and_sync(path, bytes)?;
    if let Err(err) = rename_tmp(&tmp_path, path) {
        let _ = remove_file_if_exists(&tmp_path);
        return Err(err);
    }
    // Once the temporary file has been renamed into place, keep the new contents
    // and surface the error if the parent directory cannot be made crash-safe.
    sync_parent_dir(path)?;
    Ok(())
}

pub(crate) fn write_file_atomically_and_sync_parent_budgeted(
    path: &Path,
    bytes: &[u8],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
    kind: crate::DiskReservationKind,
) -> Result<()> {
    write_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
        path,
        bytes,
        budget,
        category,
        kind,
        usize::MAX,
    )
}

pub(crate) fn write_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
    path: &Path,
    bytes: &[u8],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
    kind: crate::DiskReservationKind,
    reconciliation_memory_limit: usize,
) -> Result<()> {
    let Some(budget) = budget else {
        return write_file_atomically_and_sync_parent(path, bytes);
    };
    if !budget.governs_entry(path)? {
        return write_file_atomically_and_sync_parent(path, bytes);
    }

    let previous_bytes = crate::disk_budget::measured_path_bytes(path)?;
    let new_bytes = bytes.len() as u64;
    let reservation = budget.reserve(category, new_bytes, kind)?;
    match write_file_atomically_and_sync_parent(path, bytes) {
        Ok(()) => {
            // Do not subtract an aggregate category total for the replaced path: an external
            // writer may have changed that entry since the last scan. Conservatively charge the
            // complete new file, then obtain an exact exclusive scan for overwrites.
            let settlement = reservation.commit(new_bytes, 0);
            let reconciliation = if previous_bytes > 0 {
                budget
                    .reconcile_when_idle_with_memory_limit(reconciliation_memory_limit)
                    .map(|_| ())
            } else {
                Ok(())
            };
            match (settlement, reconciliation) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
                (Err(settlement_err), Err(reconciliation_err)) => Err(TsinkError::Other(format!(
                    "atomic file write disk settlement failed: {settlement_err}; reconciliation failed: {reconciliation_err}"
                ))),
            }
        }
        Err(write_err) => {
            // The legacy atomic helper can fail after the rename or leave an owned temporary file
            // when a lower-level write fails. Charge the full admitted peak first, then reconcile
            // if no other writer currently owns a reservation. This never understates a survivor.
            let settlement = reservation.commit_as(crate::DiskCategory::Temporary, new_bytes, 0);
            let reconciliation = budget
                .reconcile_when_idle_with_memory_limit(reconciliation_memory_limit)
                .map(|_| ());
            match (settlement, reconciliation) {
                (Ok(()), Ok(())) => Err(write_err),
                (settlement, reconciliation) => {
                    let mut errors = vec![format!("write failed: {write_err}")];
                    if let Err(err) = settlement {
                        errors.push(format!("disk settlement failed: {err}"));
                    }
                    if let Err(err) = reconciliation {
                        errors.push(format!("disk reconciliation failed: {err}"));
                    }
                    Err(TsinkError::Other(format!(
                        "atomic file write failed: {}",
                        errors.join("; ")
                    )))
                }
            }
        }
    }
}

/// Atomically writes an owned payload and releases it before any exact disk-budget scan.
///
/// Catalog publication uses this form so the encoded payload/atomic-write peak and the bounded
/// reconciliation peak are sequential rather than additive. The borrowed compatibility helper
/// above intentionally preserves its existing lifetime contract for other callers.
pub(crate) fn write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
    path: &Path,
    bytes: Vec<u8>,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
    kind: crate::DiskReservationKind,
    reconciliation_memory_limit: usize,
) -> Result<()> {
    let Some(budget) = budget else {
        return write_file_atomically_and_sync_parent(path, &bytes);
    };
    if !budget.governs_entry(path)? {
        return write_file_atomically_and_sync_parent(path, &bytes);
    }

    // Only existence matters here: replacements need an exact post-write scan, while creations
    // can commit their complete size directly. Avoid recursively measuring an unexpected
    // directory at the target name, which would otherwise introduce an unbounded pre-write scan.
    let previous_entry_existed = path_exists_no_follow(path)?;
    let new_bytes = bytes.len() as u64;
    let reservation = budget.reserve(category, new_bytes, kind)?;
    let write_result = write_file_atomically_and_sync_parent(path, &bytes);
    drop(bytes);

    match write_result {
        Ok(()) => {
            // Do not subtract an aggregate category total for the replaced path: an external
            // writer may have changed that entry since the last scan. Conservatively charge the
            // complete new file, then obtain an exact exclusive scan for overwrites.
            let settlement = reservation.commit(new_bytes, 0);
            let reconciliation = if previous_entry_existed {
                budget
                    .reconcile_when_idle_with_memory_limit(reconciliation_memory_limit)
                    .map(|_| ())
            } else {
                Ok(())
            };
            match (settlement, reconciliation) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
                (Err(settlement_err), Err(reconciliation_err)) => Err(TsinkError::Other(format!(
                    "atomic file write disk settlement failed: {settlement_err}; reconciliation failed: {reconciliation_err}"
                ))),
            }
        }
        Err(write_err) => {
            // The legacy atomic helper can fail after the rename or leave an owned temporary file
            // when a lower-level write fails. Charge the full admitted peak first, then reconcile
            // if no other writer currently owns a reservation. This never understates a survivor.
            let settlement = reservation.commit_as(crate::DiskCategory::Temporary, new_bytes, 0);
            let reconciliation = budget
                .reconcile_when_idle_with_memory_limit(reconciliation_memory_limit)
                .map(|_| ());
            match (settlement, reconciliation) {
                (Ok(()), Ok(())) => Err(write_err),
                (settlement, reconciliation) => {
                    let mut errors = vec![format!("write failed: {write_err}")];
                    if let Err(err) = settlement {
                        errors.push(format!("disk settlement failed: {err}"));
                    }
                    if let Err(err) = reconciliation {
                        errors.push(format!("disk reconciliation failed: {err}"));
                    }
                    Err(TsinkError::Other(format!(
                        "atomic file write failed: {}",
                        errors.join("; ")
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn recovery_namespace_budget_accepts_exact_cap_and_rejects_cap_plus_one() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        std::fs::write(temp_dir.path().join("known"), b"one").unwrap();
        std::fs::write(temp_dir.path().join("unknown"), b"two").unwrap();

        let entries = collect_directory_entries_bounded(temp_dir.path(), 2, "test scan")
            .expect("the exact cap must be accepted");
        assert_eq!(entries.len(), 2);

        std::fs::write(temp_dir.path().join("another-unknown"), b"three").unwrap();
        let err = collect_directory_entries_bounded(temp_dir.path(), 2, "test scan")
            .expect_err("cap plus one must be rejected");
        assert!(err.to_string().contains("2-entry global work bound"));
    }

    // Linux and other byte-oriented Unix filesystems permit opaque non-UTF-8 names. macOS APIs
    // reject these byte sequences before the scan can observe them.
    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn recovery_namespace_budget_counts_non_utf8_entries() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let temp_dir = TempDir::new().expect("tempdir should build");
        std::fs::write(temp_dir.path().join("known"), b"one").unwrap();
        std::fs::write(
            temp_dir.path().join(OsString::from_vec(vec![0xff, 0xfe])),
            b"opaque",
        )
        .unwrap();

        let err = collect_directory_entries_bounded(temp_dir.path(), 1, "non-UTF-8 test scan")
            .expect_err("an opaque second entry must still consume the cap");
        assert!(err.to_string().contains("1-entry global work bound"));
    }

    #[test]
    fn recursive_namespace_preflight_uses_one_cap_across_all_roots() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let first = temp_dir.path().join("first");
        let second = temp_dir.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        std::fs::write(first.join("one"), b"one").unwrap();
        std::fs::write(second.join("two"), b"two").unwrap();
        let roots = vec![first.clone(), second.clone()];

        validate_recursive_namespace_bounded(&roots, 2, 4, "recursive test")
            .expect("the exact aggregate cap must be accepted");

        std::fs::write(second.join("three"), b"three").unwrap();
        let err = validate_recursive_namespace_bounded(&roots, 2, 4, "recursive test")
            .expect_err("cap plus one across a later root must fail");
        assert!(err.to_string().contains("2-entry global work bound"));
    }

    #[test]
    fn recursive_namespace_preflight_rejects_excess_depth() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let root = temp_dir.path().join("root");
        std::fs::create_dir_all(root.join("one/two")).unwrap();

        let err = validate_recursive_namespace_bounded(&[root], 8, 1, "depth test")
            .expect_err("depth beyond the admitted bound must fail");
        assert!(err.to_string().contains("1-level recursive depth bound"));
    }

    #[test]
    fn recursive_namespace_plan_never_enumerates_late_descendants() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let root = temp_dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("planned"), b"planned").unwrap();
        let plan =
            validate_recursive_namespace_bounded(std::slice::from_ref(&root), 1, 4, "race test")
                .expect("the initial tree must fit its exact cap");

        std::fs::write(root.join("late"), b"late").unwrap();
        let err = plan
            .remove()
            .expect_err("a late descendant must make non-recursive removal fail");
        assert!(matches!(err, TsinkError::IoWithPath { .. }));
        assert!(root.exists());
        assert_eq!(std::fs::read(root.join("late")).unwrap(), b"late");
    }

    #[test]
    fn bounded_owned_tree_cleanup_accepts_exact_cap_and_rejects_before_deleting_cap_plus_one() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let exact = temp_dir.path().join("exact");
        std::fs::create_dir(&exact).unwrap();
        std::fs::write(exact.join("one"), b"one").unwrap();
        std::fs::write(exact.join("two"), b"two").unwrap();

        assert!(remove_owned_directory_tree_bounded_and_sync_parent(
            &exact,
            2,
            1,
            "exact cleanup test",
        )
        .expect("the exact descendant cap must be accepted"));
        assert!(!exact.exists());

        let over = temp_dir.path().join("over");
        std::fs::create_dir(&over).unwrap();
        std::fs::write(over.join("one"), b"one").unwrap();
        std::fs::write(over.join("two"), b"two").unwrap();
        let err =
            remove_owned_directory_tree_bounded_and_sync_parent(&over, 1, 1, "over cleanup test")
                .expect_err("cap plus one must fail before the first unlink");
        assert!(err.to_string().contains("1-entry global work bound"));
        assert_eq!(std::fs::read(over.join("one")).unwrap(), b"one");
        assert_eq!(std::fs::read(over.join("two")).unwrap(), b"two");
    }

    #[test]
    fn identity_checked_owned_cleanup_preserves_a_replacement_at_the_same_path() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let root = temp_dir.path().join("owned");
        let moved_owned = temp_dir.path().join("moved-owned");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("owned"), b"owned").unwrap();
        let identity = capture_plain_directory_identity(&root, "identity cleanup test").unwrap();

        std::fs::rename(&root, &moved_owned).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("foreign"), b"foreign").unwrap();

        let err = remove_owned_directory_tree_bounded_and_sync_parent_with_identity(
            &root,
            &identity,
            2,
            1,
            "identity cleanup test",
        )
        .expect_err("cleanup must reject a different directory at the owned path");
        assert!(err.to_string().contains("owned root identity changed"));
        assert_eq!(std::fs::read(root.join("foreign")).unwrap(), b"foreign");
        assert_eq!(std::fs::read(moved_owned.join("owned")).unwrap(), b"owned");
    }

    #[test]
    fn write_file_atomically_creates_missing_parent_directories() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("nested/state/series-index.bin");

        write_file_atomically_and_sync_parent(&path, b"payload").expect("atomic write should work");

        assert_eq!(
            std::fs::read(&path).expect("payload should exist"),
            b"payload"
        );
    }

    #[test]
    fn write_file_atomically_reports_parent_sync_failure_after_publication() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("state.bin");
        let _sync_failure = fail_directory_sync_once(
            temp_dir.path().to_path_buf(),
            "injected atomic publication parent sync failure",
        );

        let err = write_file_atomically_and_sync_parent(&path, b"payload")
            .expect_err("the injected parent sync failure should be reported");

        assert!(
            err.to_string()
                .contains("injected atomic publication parent sync failure"),
            "{err}"
        );
        assert_eq!(
            std::fs::read(&path).expect("published payload should remain inspectable"),
            b"payload"
        );
        assert_eq!(
            std::fs::read_dir(temp_dir.path())
                .expect("temporary directory should remain readable")
                .count(),
            1,
            "the failed publication must not leave an orphan temporary file"
        );
    }

    #[test]
    fn scoped_directory_sync_failpoint_does_not_invoke_matcher_outside_its_root() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let scoped_dir = TempDir::new().expect("scoped tempdir should build");
        let unrelated_dir = TempDir::new().expect("unrelated tempdir should build");
        let matcher_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&matcher_calls);
        let guard = fail_directory_sync_matching_once_under(
            scoped_dir.path().to_path_buf(),
            move |_| {
                observed_calls.fetch_add(1, Ordering::SeqCst);
                false
            },
            "the non-failing matcher should not inject an error",
        );

        sync_dir(unrelated_dir.path()).expect("an unrelated directory sync should succeed");
        assert_eq!(
            matcher_calls.load(Ordering::SeqCst),
            0,
            "a process-global hook must not invoke a scoped matcher for another test's path"
        );

        sync_dir(scoped_dir.path()).expect("the scoped directory sync should succeed");
        assert_eq!(
            matcher_calls.load(Ordering::SeqCst),
            1,
            "the scoped matcher must still observe directory syncs beneath its own root"
        );
        drop(guard);
    }

    #[test]
    fn write_tmp_and_sync_uses_unique_paths_for_same_target() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("state.bin");

        let first = write_tmp_and_sync(&path, b"first").expect("first temp write should work");
        let second = write_tmp_and_sync(&path, b"second").expect("second temp write should work");

        assert_ne!(first, second);
        assert!(first.exists());
        assert!(second.exists());

        rename_tmp(&first, &path).expect("first rename should work");
        assert_eq!(
            std::fs::read(&path).expect("first payload should exist"),
            b"first"
        );
        rename_tmp(&second, &path).expect("second rename should work");
        assert_eq!(
            std::fs::read(&path).expect("second payload should exist"),
            b"second"
        );
    }

    #[test]
    fn unique_staging_directory_allocation_is_create_exclusive() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let target = temp_dir.path().join("snapshot");

        let first = create_unique_staging_dir(&target, "snapshot")
            .expect("first staging directory should be created");
        let second = create_unique_staging_dir(&target, "snapshot")
            .expect("second staging directory should be created");

        assert_ne!(first, second);
        for staging in [&first, &second] {
            let metadata =
                std::fs::symlink_metadata(staging).expect("staging directory should exist");
            assert!(metadata.file_type().is_dir());
            assert!(!is_link_or_reparse_point(&metadata));
            assert_eq!(
                staging.parent(),
                Some(temp_dir.path()),
                "staging must be a sibling of its target"
            );
        }
    }

    #[test]
    fn exact_staging_directory_creation_never_reuses_a_raced_entry() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let target = temp_dir.path().join("restore");
        let staging =
            stage_dir_path(&target, "restore-staging").expect("staging path should be planned");
        std::fs::create_dir(&staging).expect("racer should create the planned entry");
        std::fs::write(staging.join("foreign"), b"foreign").expect("foreign payload should write");

        let err = create_staging_dir_exclusive(&staging)
            .expect_err("create-exclusive must reject the raced entry");

        assert!(matches!(
            err,
            TsinkError::IoWithPath { ref source, .. }
                if source.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(
            std::fs::read(staging.join("foreign")).expect("foreign payload must survive"),
            b"foreign"
        );
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple",
        windows
    ))]
    #[test]
    fn no_replace_rename_preserves_a_raced_destination() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let source = temp_dir.path().join("owned-staging");
        let destination = temp_dir.path().join("raced-destination");
        std::fs::create_dir(&source).expect("source should exist");
        std::fs::write(source.join("owned"), b"owned").expect("owned payload should write");
        std::fs::create_dir(&destination).expect("raced destination should exist");
        std::fs::write(destination.join("foreign"), b"foreign")
            .expect("foreign payload should write");

        rename_path_noreplace(&source, &destination)
            .expect_err("no-replace rename must reject the raced destination");

        assert_eq!(
            std::fs::read(source.join("owned")).expect("source must survive"),
            b"owned"
        );
        assert_eq!(
            std::fs::read(destination.join("foreign")).expect("destination must survive"),
            b"foreign"
        );
    }

    #[test]
    fn write_file_atomically_handles_parallel_writers_for_same_target() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = Arc::new(temp_dir.path().join("shared-state.bin"));
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();

        for worker in 0..8 {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for iteration in 0..32 {
                    let payload = format!("worker-{worker}-iteration-{iteration}");
                    write_file_atomically_and_sync_parent(&path, payload.as_bytes())
                        .expect("parallel atomic write should succeed");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("writer should not panic");
        }

        let payload = std::fs::read_to_string(path.as_ref()).expect("final payload should exist");
        assert!(payload.starts_with("worker-"));
    }

    #[test]
    fn budgeted_removal_reconciles_exact_usage_while_over_limit() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::write(wal_dir.join("wal-0000000000000000.log"), vec![1u8; 11]).unwrap();
        let unknown_path = temp_dir.path().join("host-owned.bin");
        std::fs::write(&unknown_path, vec![2u8; 7]).unwrap();
        let budget = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(1),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        assert!(budget.snapshot().over_limit);

        remove_path_if_exists_and_sync_parent_budgeted(
            &wal_dir,
            Some(&budget),
            crate::DiskCategory::Wal,
        )
        .unwrap();

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 7);
        assert_eq!(snapshot.unknown_bytes, 7);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
        assert_eq!(snapshot.reconciliations_total, 2);
        assert!(unknown_path.exists());
    }

    #[test]
    fn budgeted_rename_rolls_back_after_parent_sync_failure() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let source = temp_dir.path().join(".tmp-tsink-promotion.bin");
        let destination = temp_dir.path().join("published.bin");
        std::fs::write(&source, b"staged").expect("staged payload should write");
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .expect("disk budget should open");
        let _sync_failure = fail_directory_sync_once(
            temp_dir.path().to_path_buf(),
            "injected publication directory sync failure",
        );

        let err = rename_and_sync_parents_budgeted_reclassify(
            &source,
            &destination,
            Some(&budget),
            crate::DiskCategory::Temporary,
            crate::DiskCategory::Segments,
        )
        .expect_err("publication should report the injected sync failure");

        assert!(err
            .to_string()
            .contains("injected publication directory sync failure"));
        assert_eq!(std::fs::read(&source).unwrap(), b"staged");
        assert!(!path_exists_no_follow(&destination).unwrap());
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 6);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == crate::DiskCategory::Temporary)
                .map(|usage| usage.bytes),
            Some(6)
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn budgeted_atomic_write_cleans_an_injected_partial_storage_full_write() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let target = temp_dir.path().join("series_index.bin");
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .expect("disk budget should open");
        let _write_failure = fail_tmp_write_after_bytes_once(
            target.clone(),
            3,
            std::io::ErrorKind::StorageFull,
            "injected filesystem-full short write",
        );

        let err = write_file_atomically_and_sync_parent_budgeted(
            &target,
            b"0123456789",
            Some(&budget),
            crate::DiskCategory::Registry,
            crate::DiskReservationKind::Growth,
        )
        .expect_err("the injected partial write should fail");

        assert!(matches!(
            err,
            TsinkError::Io(ref source) if source.kind() == std::io::ErrorKind::StorageFull
        ));
        assert!(!path_exists_no_follow(&target).unwrap());
        assert_eq!(std::fs::read_dir(temp_dir.path()).unwrap().count(), 0);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.reconciliations_total, 2);
    }

    #[cfg(unix)]
    #[test]
    fn budgeted_atomic_write_replaces_an_in_tree_symlink_without_touching_its_target() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let data_root = temp_dir.path().join("data");
        let outside = temp_dir.path().join("outside.bin");
        std::fs::create_dir_all(&data_root).unwrap();
        std::fs::write(&outside, b"outside").unwrap();
        let managed_entry = data_root.join("series_index.bin");
        symlink(&outside, &managed_entry).unwrap();
        let budget =
            crate::LocalDiskBudget::open(&data_root, crate::LocalDiskLimits::default()).unwrap();

        assert!(!budget.governs(&managed_entry).unwrap());
        assert!(budget.governs_entry(&managed_entry).unwrap());
        write_file_atomically_and_sync_parent_budgeted(
            &managed_entry,
            b"replacement",
            Some(&budget),
            crate::DiskCategory::Registry,
            crate::DiskReservationKind::Maintenance,
        )
        .unwrap();

        assert_eq!(std::fs::read(&managed_entry).unwrap(), b"replacement");
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
        assert!(!std::fs::symlink_metadata(&managed_entry)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(budget.snapshot().active_reservations, 0);
    }

    #[cfg(unix)]
    #[test]
    fn path_exists_no_follow_detects_dangling_symlinks() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let link = temp_dir.path().join("dangling");

        symlink(temp_dir.path().join("missing-target"), &link).expect("dangling symlink");

        assert!(!link.exists(), "Path::exists follows the missing target");
        assert!(path_exists_no_follow(&link).expect("symlink metadata should load"));
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_helpers_reject_symlink_roots() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let source = temp_dir.path().join("source");
        std::fs::create_dir_all(&source).expect("source should exist");
        std::fs::write(source.join("payload.bin"), b"payload").expect("payload should write");

        let source_link = temp_dir.path().join("source-link");
        symlink(&source, &source_link).expect("directory symlink");

        let snapshot_dest = temp_dir.path().join("snapshot-dest");
        let snapshot_err =
            copy_dir_if_exists(&source_link, &snapshot_dest).expect_err("symlink root must fail");
        assert!(matches!(snapshot_err, TsinkError::InvalidConfiguration(_)));
        assert!(!snapshot_dest.exists());

        let restore_err =
            measure_restore_directory(&source_link).expect_err("symlink root must fail");
        assert!(matches!(restore_err, TsinkError::InvalidConfiguration(_)));
    }
}
