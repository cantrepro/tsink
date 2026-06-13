use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Result, TsinkError};

static STAGE_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
type DirectorySyncHook = dyn Fn(&Path) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
pub(crate) struct DirectorySyncHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
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
        let mut writer = BufWriter::new(file);
        writer.write_all(bytes)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        return Ok(tmp_path);
    }

    Err(TsinkError::Other(format!(
        "failed to reserve unique temporary file for {}",
        path.display()
    )))
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
