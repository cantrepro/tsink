use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{Result, TsinkError};

static STAGE_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
type DirectorySyncHook = dyn Fn(&Path) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
type TmpWriteFailureHook =
    dyn Fn(&Path, &mut std::fs::File, &[u8]) -> Option<TsinkError> + Send + Sync + 'static;

#[cfg(test)]
pub(crate) struct DirectorySyncHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
struct TmpWriteFailureHookGuard {
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
fn directory_sync_hook_slot() -> &'static std::sync::Mutex<Option<std::sync::Arc<DirectorySyncHook>>>
{
    static DIRECTORY_SYNC_HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<std::sync::Arc<DirectorySyncHook>>>,
    > = std::sync::OnceLock::new();
    DIRECTORY_SYNC_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn directory_sync_test_lock() -> &'static std::sync::Mutex<()> {
    static DIRECTORY_SYNC_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    DIRECTORY_SYNC_TEST_LOCK.get_or_init(|| std::sync::Mutex::new(()))
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
fn fail_tmp_write_after_bytes_once(
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

pub(crate) fn path_exists_no_follow(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
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

pub(crate) fn remove_dir_if_exists(path: &Path) -> std::io::Result<bool> {
    match remove_dir_all_with_retry(path) {
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
    let Some(budget) = budget else {
        return remove_path_if_exists_and_sync_parent(path);
    };
    if !budget.governs_entry(path)? {
        return remove_path_if_exists_and_sync_parent(path);
    }

    let reservation = budget.reserve(category, 0, crate::DiskReservationKind::Recovery)?;
    let removal_result = remove_path_if_exists_and_sync_parent(path);
    let settlement_result = reservation.commit(0, 0);
    let reconciliation_result = budget.reconcile_when_idle().map(|_| ());

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

pub(crate) fn stage_dir_path(target: &Path, purpose: &str) -> Result<PathBuf> {
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

    for _ in 0..256 {
        let nonce = STAGE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".tmp-tsink-{purpose}-{target_name}-{nonce:016x}"));
        if !path_exists_no_follow(&candidate)? {
            return Ok(candidate);
        }
    }

    Err(TsinkError::Other(format!(
        "failed to allocate unique staging path for {}",
        target.display()
    )))
}

pub(crate) fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)?;
    if !metadata.is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "expected directory while copying {}, found non-directory",
            source.display()
        )));
    }

    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let entry_type = entry.file_type()?;
        let entry_source = entry.path();
        let entry_destination = destination.join(entry.file_name());

        if entry_type.is_dir() {
            copy_dir_recursive(&entry_source, &entry_destination)?;
        } else if entry_type.is_file() {
            std::fs::copy(&entry_source, &entry_destination)?;
        } else {
            return Err(TsinkError::InvalidConfiguration(format!(
                "unsupported non-file entry while copying snapshot: {}",
                entry_source.display()
            )));
        }
    }

    Ok(())
}

pub(crate) fn copy_dir_if_exists(source: &Path, destination: &Path) -> Result<()> {
    match std::fs::symlink_metadata(source) {
        Ok(metadata) => {
            if !metadata.is_dir() {
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

pub(crate) fn copy_dir_contents(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)?;
    if !metadata.is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a directory: {}",
            source.display()
        )));
    }

    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let entry_type = entry.file_type()?;
        let entry_source = entry.path();
        let entry_destination = destination.join(entry.file_name());

        if entry_type.is_dir() {
            copy_dir_recursive(&entry_source, &entry_destination)?;
        } else if entry_type.is_file() {
            std::fs::copy(&entry_source, &entry_destination)?;
        } else {
            return Err(TsinkError::InvalidConfiguration(format!(
                "unsupported non-file entry while restoring snapshot: {}",
                entry_source.display()
            )));
        }
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
    let Some(parent) = path.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "temporary file target has no parent directory: {}",
            path.display()
        )));
    };
    std::fs::create_dir_all(parent)?;

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
                budget.reconcile_when_idle().map(|_| ())
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
            let reconciliation = budget.reconcile_when_idle().map(|_| ());
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

        let restore_dest = temp_dir.path().join("restore-dest");
        let restore_err =
            copy_dir_contents(&source_link, &restore_dest).expect_err("symlink root must fail");
        assert!(matches!(restore_err, TsinkError::InvalidConfiguration(_)));
        assert!(!restore_dest.exists());
    }
}
