use super::*;
use std::ffi::OsString;
use std::time::{Duration, Instant};

const DATA_PATH_LOCK_FILE_NAME: &str = ".tsink.lock";
const SHARED_OBJECT_STORE_WRITER_LOCK_FILE_NAME: &str = ".tsink-writer.lock";
const LOCK_ACQUIRE_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const LOCK_ACQUIRE_RETRY_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub(super) struct DataPathProcessLock {
    _inner: PathProcessLock,
}

#[derive(Debug)]
pub(super) struct SharedObjectStoreProcessLock {
    _inner: PathProcessLock,
}

#[derive(Debug)]
struct PathProcessLock {
    requested_root: PathBuf,
    root_identity: same_file::Handle,
    _lock_path: PathBuf,
    lock_file: std::fs::File,
    identity: same_file::Handle,
}

impl DataPathProcessLock {
    pub(super) fn acquire(data_path: &Path) -> Result<Self> {
        PathProcessLock::acquire(data_path, DATA_PATH_LOCK_FILE_NAME, "data path")
            .map(|inner| Self { _inner: inner })
    }
}

impl SharedObjectStoreProcessLock {
    pub(super) fn acquire(object_store_root: &Path) -> Result<Self> {
        PathProcessLock::acquire(
            object_store_root,
            SHARED_OBJECT_STORE_WRITER_LOCK_FILE_NAME,
            "shared object-store writer root",
        )
        .map(|inner| Self { _inner: inner })
    }

    pub(super) fn validate(&self) -> Result<()> {
        self._inner
            .validate_identity("shared object-store writer root")
    }
}

impl PathProcessLock {
    fn acquire(root: &Path, lock_file_name: &str, description: &str) -> Result<Self> {
        let requested_root = absolute_root_path(root)?;
        let root = ensure_real_directory_tree(&requested_root, description)?;
        let root_identity =
            same_file::Handle::from_path(&root).map_err(|source| TsinkError::IoWithPath {
                path: root.clone(),
                source,
            })?;
        let lock_path = root.join(lock_file_name);
        match std::fs::symlink_metadata(&lock_path) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(metadata) => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "{description} lock must be a regular file, found {:?}: {}",
                    metadata.file_type(),
                    lock_path.display()
                )))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: lock_path,
                    source,
                })
            }
        }
        let lock_file = open_lock_file_no_follow(&lock_path, true).map_err(|source| {
            TsinkError::IoWithPath {
                path: lock_path.clone(),
                source,
            }
        })?;
        let opened_metadata = lock_file
            .metadata()
            .map_err(|source| TsinkError::IoWithPath {
                path: lock_path.clone(),
                source,
            })?;
        if !opened_metadata.file_type().is_file()
            || crate::engine::fs_utils::is_link_or_reparse_point(&opened_metadata)
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{description} lock must remain a regular file while opening: {}",
                lock_path.display()
            )));
        }

        let deadline = Instant::now() + LOCK_ACQUIRE_RETRY_TIMEOUT;
        loop {
            match lock_file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::InvalidConfiguration(format!(
                            "{description} {} is already locked by another tsink process ({})",
                            root.display(),
                            lock_path.display()
                        )));
                    }
                    std::thread::sleep(LOCK_ACQUIRE_RETRY_INTERVAL);
                }
                Err(std::fs::TryLockError::Error(source)) => {
                    return Err(TsinkError::IoWithPath {
                        path: lock_path.clone(),
                        source,
                    });
                }
            }
        }

        let identity = same_file::Handle::from_file(lock_file.try_clone().map_err(|source| {
            TsinkError::IoWithPath {
                path: lock_path.clone(),
                source,
            }
        })?)
        .map_err(|source| TsinkError::IoWithPath {
            path: lock_path.clone(),
            source,
        })?;

        let acquired = Self {
            requested_root,
            root_identity,
            _lock_path: lock_path,
            lock_file,
            identity,
        };
        acquired.validate_identity(description)?;
        Ok(acquired)
    }

    fn validate_identity(&self, description: &str) -> Result<()> {
        let root_metadata = std::fs::symlink_metadata(&self.requested_root).map_err(|source| {
            TsinkError::IoWithPath {
                path: self.requested_root.clone(),
                source,
            }
        })?;
        if !root_metadata.file_type().is_dir()
            || crate::engine::fs_utils::is_link_or_reparse_point(&root_metadata)
        {
            return Err(TsinkError::DataCorruption(format!(
                "{description} path changed type while its writer lease was held: {}",
                self.requested_root.display()
            )));
        }
        let current_root_identity =
            same_file::Handle::from_path(&self.requested_root).map_err(|source| {
                TsinkError::IoWithPath {
                    path: self.requested_root.clone(),
                    source,
                }
            })?;
        if current_root_identity != self.root_identity {
            return Err(TsinkError::DataCorruption(format!(
                "{description} directory identity changed while its writer lease was held: {}",
                self.requested_root.display()
            )));
        }

        let metadata = std::fs::symlink_metadata(&self._lock_path).map_err(|source| {
            TsinkError::IoWithPath {
                path: self._lock_path.clone(),
                source,
            }
        })?;
        if !metadata.file_type().is_file()
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        {
            return Err(TsinkError::DataCorruption(format!(
                "{description} lock path changed type while held: {}",
                self._lock_path.display()
            )));
        }
        let current = open_lock_file_no_follow(&self._lock_path, false).map_err(|source| {
            TsinkError::IoWithPath {
                path: self._lock_path.clone(),
                source,
            }
        })?;
        let current_identity =
            same_file::Handle::from_file(current).map_err(|source| TsinkError::IoWithPath {
                path: self._lock_path.clone(),
                source,
            })?;
        if current_identity != self.identity {
            return Err(TsinkError::DataCorruption(format!(
                "{description} lock file identity changed while held: {}",
                self._lock_path.display()
            )));
        }
        Ok(())
    }
}

fn absolute_root_path(root: &Path) -> Result<PathBuf> {
    if root.is_absolute() {
        Ok(root.to_path_buf())
    } else {
        Ok(std::env::current_dir().map_err(TsinkError::Io)?.join(root))
    }
}

fn open_lock_file_no_follow(path: &Path, create: bool) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(create)
        .truncate(false);
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
    options.open(path)
}

fn ensure_real_directory_tree(root: &Path, description: &str) -> Result<PathBuf> {
    let root = absolute_root_path(root)?;
    let mut current = root.as_path();
    let mut missing = Vec::<OsString>::new();
    loop {
        match std::fs::symlink_metadata(current) {
            Ok(metadata)
                if metadata.file_type().is_dir()
                    && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
            {
                break;
            }
            // Ancestor aliases such as macOS `/var -> private/var` are normal platform paths.
            // The configured root itself is never accepted as an alias, but a nearest existing
            // ancestor is canonicalized before any missing descendant is created.
            Ok(metadata)
                if current != root
                    && crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
            {
                break;
            }
            Ok(metadata) => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "{description} must be a real directory, found {:?}: {}",
                    metadata.file_type(),
                    current.display()
                )))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let component = current.file_name().ok_or_else(|| {
                    TsinkError::InvalidConfiguration(format!(
                        "{description} has no existing directory ancestor: {}",
                        root.display()
                    ))
                })?;
                missing.push(component.to_os_string());
                current = current.parent().ok_or_else(|| {
                    TsinkError::InvalidConfiguration(format!(
                        "{description} has no existing directory ancestor: {}",
                        root.display()
                    ))
                })?;
            }
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: current.to_path_buf(),
                    source,
                })
            }
        }
    }

    let mut created = std::fs::canonicalize(current).map_err(|source| TsinkError::IoWithPath {
        path: current.to_path_buf(),
        source,
    })?;
    let ancestor_metadata =
        std::fs::symlink_metadata(&created).map_err(|source| TsinkError::IoWithPath {
            path: created.clone(),
            source,
        })?;
    if !ancestor_metadata.file_type().is_dir()
        || crate::engine::fs_utils::is_link_or_reparse_point(&ancestor_metadata)
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{description} has no real directory ancestor: {}",
            root.display()
        )));
    }
    for component in missing.iter().rev() {
        created.push(component);
        match std::fs::create_dir(&created) {
            Ok(()) => crate::engine::fs_utils::sync_parent_dir(&created)?,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: created,
                    source,
                })
            }
        }
        let metadata =
            std::fs::symlink_metadata(&created).map_err(|source| TsinkError::IoWithPath {
                path: created.clone(),
                source,
            })?;
        if !metadata.file_type().is_dir()
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{description} creation encountered a link-like or non-directory entry: {}",
                created.display()
            )));
        }
    }
    Ok(created)
}

impl Drop for PathProcessLock {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}
