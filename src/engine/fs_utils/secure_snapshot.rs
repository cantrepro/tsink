#[cfg(test)]
use super::{invoke_directory_sync_hook, invoke_file_sync_hook};
use super::{is_link_or_reparse_point, RestoreDirectoryMeasurement};
use crate::{Result, TsinkError};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
#[cfg(any(unix, windows))]
use std::fs::OpenOptions;
use std::fs::{File, Metadata, Permissions};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use std::ffi::{CStr, CString};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

pub(crate) const MAX_SECURE_SNAPSHOT_SESSION_RETAINED_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_SECURE_SNAPSHOT_OPERATION_RETAINED_BYTES: usize =
    2 * MAX_SECURE_SNAPSHOT_SESSION_RETAINED_BYTES;
const MAX_SECURE_SNAPSHOT_RELATIVE_PATH_BYTES: usize = 32 * 1024;

/// One securely opened and measured snapshot source tree.
///
/// The source root and every later descendant open are anchored to no-follow directory handles.
/// The retained manifest binds each relative path to the identity observed during measurement, so
/// copying never trusts a pathname-only type or length check.
#[derive(Debug)]
pub(crate) struct SecureSnapshotSourceTree {
    root: SecureDirectoryAnchor,
    root_identity: ClosedSnapshotIdentity,
    measurement: RestoreDirectoryMeasurement,
    entries: Vec<SecureSourceEntry>,
    retained_memory_bytes: usize,
}

/// One securely opened regular-file source outside a measured tree.
#[derive(Debug)]
pub(crate) struct SecureSnapshotSourceFile {
    parent: SecureDirectoryAnchor,
    parent_identity: ClosedSnapshotIdentity,
    file_name: OsString,
    display_path: PathBuf,
    len: u64,
    permissions: Permissions,
    identity: ClosedSnapshotIdentity,
    retained_memory_bytes: usize,
}

/// A lightweight requested-path binding used to keep several independently measured source roots
/// in one namespace generation.
#[derive(Debug)]
pub(crate) struct SecureSnapshotNamespaceFence {
    anchor: SecureDirectoryAnchor,
    identity: ClosedSnapshotIdentity,
    retained_memory_bytes: usize,
}

/// A create-exclusive staging directory whose descendants are created relative to its handle.
///
/// Each retained identity comes from the handle opened at creation time. Verification enumerates
/// the directory through the retained root and rejects unknown, missing, replaced, or type-changed
/// entries before a caller crosses a later publication boundary.
#[derive(Debug)]
pub(crate) struct SecureSnapshotStagingDirectory {
    parent: SecureDirectoryAnchor,
    root: SecureDirectoryAnchor,
    display_path: PathBuf,
    root_name: OsString,
    created: Vec<SecureCreatedEntry>,
    created_directory_index: HashMap<u64, usize>,
    created_directory_index_bytes: usize,
    created_manifest_bytes: usize,
    retained_memory_bytes: usize,
    operation_baseline_retained_bytes: usize,
}

/// The pre-move closed-identity manifest for an original restore target now visible as a backup.
///
/// Cleanup consumes this token and removes only entries that still match the manifest captured
/// before the target-to-backup move.
#[derive(Debug)]
pub(crate) struct SecureSnapshotPublishedBackup {
    parent: SecureDirectoryAnchor,
    root: SecureDirectoryAnchor,
    requested_backup_path: PathBuf,
    backup_path: PathBuf,
    backup_name: OsString,
    root_identity: ClosedSnapshotIdentity,
    measurement: RestoreDirectoryMeasurement,
    entries: Vec<SecureSourceEntry>,
    retained_memory_bytes: usize,
    external_operation_baseline_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct SecureSnapshotPublicationError {
    pub(crate) error: TsinkError,
    /// True once the staging directory has become visible at the requested target.
    pub(crate) published: bool,
}

#[derive(Debug, Clone, Copy)]
struct SecureSnapshotReplacementPaths<'a> {
    requested_target: &'a Path,
    target: &'a Path,
    target_name: &'a OsStr,
    requested_backup: &'a Path,
    backup: &'a Path,
    backup_name: &'a OsStr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecureSourceKind {
    Directory,
    RegularFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbedEntryKind {
    Directory,
    RegularFile,
    LinkLike,
    Other,
}

#[derive(Debug)]
struct SecureSourceEntry {
    relative: PathBuf,
    kind: SecureSourceKind,
    len: u64,
    permissions: Permissions,
    identity: ClosedSnapshotIdentity,
}

#[derive(Debug)]
struct SecureCreatedEntry {
    relative: PathBuf,
    kind: SecureSourceKind,
    identity: ClosedSnapshotIdentity,
}

/// A closed identity captured from an actual open file or directory handle.
///
/// No descriptor/handle is retained per manifest entry. Unix ctime and Windows last-write time,
/// together with length and type, detect ordinary same-object mutations in addition to entry
/// replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClosedSnapshotIdentity {
    volume_or_device: u64,
    file_index_or_inode: u64,
    change_time: u64,
    change_time_subsecond: u64,
    length: u64,
    kind: u8,
}

impl ClosedSnapshotIdentity {
    fn stable_eq(self, other: Self) -> bool {
        self.volume_or_device == other.volume_or_device
            && self.file_index_or_inode == other.file_index_or_inode
            && self.kind == other.kind
    }

    #[cfg(unix)]
    fn from_open_file(file: &File, display: &Path) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })?;
        Ok(Self {
            volume_or_device: metadata.dev(),
            file_index_or_inode: metadata.ino(),
            change_time: metadata.ctime() as u64,
            change_time_subsecond: metadata.ctime_nsec() as u64,
            length: metadata.len(),
            kind: metadata_kind_tag(&metadata),
        })
    }

    #[cfg(windows)]
    fn from_open_file(file: &File, display: &Path) -> Result<Self> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })?;
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        let succeeded = unsafe {
            GetFileInformationByHandle(
                file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
                &mut information,
            )
        };
        if succeeded == 0 {
            return Err(TsinkError::IoWithPath {
                path: display.to_path_buf(),
                source: std::io::Error::last_os_error(),
            });
        }
        Ok(Self {
            volume_or_device: u64::from(information.dwVolumeSerialNumber),
            file_index_or_inode: (u64::from(information.nFileIndexHigh) << 32)
                | u64::from(information.nFileIndexLow),
            change_time: (u64::from(information.ftLastWriteTime.dwHighDateTime) << 32)
                | u64::from(information.ftLastWriteTime.dwLowDateTime),
            change_time_subsecond: 0,
            length: (u64::from(information.nFileSizeHigh) << 32)
                | u64::from(information.nFileSizeLow),
            kind: metadata_kind_tag(&metadata),
        })
    }

    #[cfg(not(any(unix, windows)))]
    fn from_open_file(_file: &File, display: &Path) -> Result<Self> {
        Err(TsinkError::InvalidConfiguration(format!(
            "secure snapshot identity is unsupported on this platform: {}",
            display.display()
        )))
    }
}

fn metadata_kind_tag(metadata: &Metadata) -> u8 {
    if is_link_or_reparse_point(metadata) {
        3
    } else if metadata.file_type().is_dir() {
        1
    } else if metadata.file_type().is_file() {
        2
    } else {
        4
    }
}

#[derive(Debug)]
struct SecureDirectoryAnchor {
    display_path: PathBuf,
    root: File,
    #[cfg(windows)]
    ancestor_locks: Vec<File>,
}

#[derive(Debug)]
struct SecureOpenedDirectory {
    file: File,
    #[cfg(windows)]
    component_locks: Vec<File>,
}

#[derive(Debug)]
struct SecureOpenedFile {
    file: File,
    #[cfg(windows)]
    component_locks: Vec<File>,
}

#[cfg(all(test, unix))]
type AnchorResolutionHook = dyn Fn(&Path) + Send + Sync + 'static;

#[cfg(all(test, unix))]
fn anchor_resolution_hook_slot(
) -> &'static std::sync::Mutex<Option<std::sync::Arc<AnchorResolutionHook>>> {
    static HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<std::sync::Arc<AnchorResolutionHook>>>,
    > = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(all(test, unix))]
struct AnchorResolutionHookGuard;

#[cfg(all(test, unix))]
impl Drop for AnchorResolutionHookGuard {
    fn drop(&mut self) {
        *anchor_resolution_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(all(test, unix))]
fn install_anchor_resolution_hook(
    hook: impl Fn(&Path) + Send + Sync + 'static,
) -> AnchorResolutionHookGuard {
    *anchor_resolution_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(std::sync::Arc::new(hook));
    AnchorResolutionHookGuard
}

#[cfg(all(test, unix))]
fn invoke_anchor_resolution_hook(path: &Path) {
    let hook = anchor_resolution_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(path);
    }
}

#[cfg(all(test, unix))]
type PublicationAfterRenameHook = dyn Fn(&Path) + Send + Sync + 'static;

#[cfg(all(test, unix))]
fn publication_after_rename_hook_slot(
) -> &'static std::sync::Mutex<Option<std::sync::Arc<PublicationAfterRenameHook>>> {
    static HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<std::sync::Arc<PublicationAfterRenameHook>>>,
    > = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(all(test, unix))]
fn publication_after_rename_test_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(all(test, unix))]
struct PublicationAfterRenameHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(all(test, unix))]
impl Drop for PublicationAfterRenameHookGuard {
    fn drop(&mut self) {
        *publication_after_rename_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(all(test, unix))]
fn install_publication_after_rename_hook(
    hook: impl Fn(&Path) + Send + Sync + 'static,
) -> PublicationAfterRenameHookGuard {
    let lock = publication_after_rename_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    *publication_after_rename_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(std::sync::Arc::new(hook));
    PublicationAfterRenameHookGuard { _lock: lock }
}

#[cfg(all(test, unix))]
fn invoke_publication_after_rename_hook(path: &Path) {
    let hook = publication_after_rename_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(path);
    }
}

impl SecureSnapshotSourceTree {
    pub(crate) fn open_and_measure(path: &Path) -> Result<Self> {
        Self::open_and_measure_with_operation_baseline(path, 0)
    }

    pub(crate) fn open_and_measure_with_operation_baseline(
        path: &Path,
        operation_baseline_retained_bytes: usize,
    ) -> Result<Self> {
        let root = open_existing_directory_chain_nofollow(path, "snapshot source")?;
        Self::measure_opened_root(path, root, operation_baseline_retained_bytes)
    }

    pub(crate) fn open_optional_and_measure_with_operation_baseline(
        path: &Path,
        operation_baseline_retained_bytes: usize,
    ) -> Result<Option<Self>> {
        let root = match open_existing_directory_chain_nofollow(path, "snapshot source") {
            Ok(root) => root,
            Err(TsinkError::IoWithPath { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        Self::measure_opened_root(path, root, operation_baseline_retained_bytes).map(Some)
    }

    fn measure_opened_root(
        path: &Path,
        root: SecureDirectoryAnchor,
        operation_baseline_retained_bytes: usize,
    ) -> Result<Self> {
        let fixed_bytes = source_tree_retained_bytes(&root, 0)?;
        let measurement_baseline = checked_add_memory(
            operation_baseline_retained_bytes,
            fixed_bytes,
            "secure snapshot aggregate source measurement",
        )?;
        admit_operation_retained_bytes(
            operation_baseline_retained_bytes,
            fixed_bytes,
            "secure snapshot aggregate source measurement",
            path,
        )?;
        let (measurement, entries, manifest_bytes) = measure_tree(&root, measurement_baseline)?;
        let root_identity = identity_from_file(&root.root, path)?;
        let retained_memory_bytes = source_tree_retained_bytes(&root, manifest_bytes)?;
        admit_session_retained_bytes(
            retained_memory_bytes,
            "secure snapshot source manifest",
            path,
        )?;
        admit_operation_retained_bytes(
            operation_baseline_retained_bytes,
            retained_memory_bytes,
            "secure snapshot aggregate source sessions",
            path,
        )?;
        Ok(Self {
            root,
            root_identity,
            measurement,
            entries,
            retained_memory_bytes,
        })
    }

    pub(crate) fn measurement(&self) -> RestoreDirectoryMeasurement {
        self.measurement
    }

    pub(crate) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_bytes
    }

    pub(crate) fn measured_directory_exists(&self, relative: &Path) -> Result<bool> {
        validate_normalized_relative(relative, false)?;
        match self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(relative))
        {
            Ok(index) if self.entries[index].kind == SecureSourceKind::Directory => Ok(true),
            Ok(_) => Err(TsinkError::InvalidConfiguration(format!(
                "snapshot canonical directory path is a regular file: {}",
                self.root.display_path.join(relative).display()
            ))),
            Err(_) => Ok(false),
        }
    }

    pub(crate) fn measured_regular_file_exists(&self, relative: &Path) -> Result<bool> {
        validate_normalized_relative(relative, false)?;
        match self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(relative))
        {
            Ok(index) if self.entries[index].kind == SecureSourceKind::RegularFile => Ok(true),
            Ok(_) => Err(TsinkError::InvalidConfiguration(format!(
                "snapshot canonical regular-file path is a directory: {}",
                self.root.display_path.join(relative).display()
            ))),
            Err(_) => Ok(false),
        }
    }

    fn open_measured_regular_file(&self, relative: &Path) -> Result<SecureOpenedFile> {
        validate_normalized_relative(relative, false)?;
        let index = self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(relative))
            .map_err(|_| {
                TsinkError::InvalidConfiguration(format!(
                    "snapshot is missing required regular file: {}",
                    self.root.display_path.join(relative).display()
                ))
            })?;
        let entry = &self.entries[index];
        if entry.kind != SecureSourceKind::RegularFile {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot required file is not a regular file: {}",
                self.root.display_path.join(relative).display()
            )));
        }
        let display_path = self.root.display_path.join(relative);
        let opened = open_relative_regular_file(
            &self.root,
            relative,
            self.parent_identity(relative)?,
            &entry.identity,
        )?;
        validate_opened_source_file(&opened.file, &entry.identity, entry.len, &display_path)?;
        Ok(opened)
    }

    pub(crate) fn read_measured_file_bounded(
        &self,
        relative: &Path,
        max_len: usize,
    ) -> Result<Vec<u8>> {
        validate_normalized_relative(relative, false)?;
        let index = self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(relative))
            .map_err(|_| {
                TsinkError::InvalidConfiguration(format!(
                    "snapshot is missing required regular file: {}",
                    self.root.display_path.join(relative).display()
                ))
            })?;
        let entry = &self.entries[index];
        if entry.kind != SecureSourceKind::RegularFile {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot required file is not a regular file: {}",
                self.root.display_path.join(relative).display()
            )));
        }
        let length = usize::try_from(entry.len).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "snapshot required file length exceeds this platform's addressable range: {}",
                self.root.display_path.join(relative).display()
            ))
        })?;
        if length > max_len {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot required file uses {length} bytes, exceeding limit {max_len}: {}",
                self.root.display_path.join(relative).display()
            )));
        }
        admit_operation_retained_bytes(
            self.retained_memory_bytes,
            length,
            "secure snapshot measured-file read buffer",
            &self.root.display_path.join(relative),
        )?;
        let mut opened = self.open_measured_regular_file(relative)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| {
            TsinkError::Other(format!(
                "unable to allocate snapshot required-file buffer: {}",
                self.root.display_path.join(relative).display()
            ))
        })?;
        (&mut opened.file)
            .take(entry.len)
            .read_to_end(&mut bytes)
            .map_err(|source| TsinkError::IoWithPath {
                path: self.root.display_path.join(relative),
                source,
            })?;
        if bytes.len() != length {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot required file shrank while reading: {} yielded {} of {} bytes",
                self.root.display_path.join(relative).display(),
                bytes.len(),
                entry.len
            )));
        }
        let mut probe = [0u8; 1];
        if opened
            .file
            .read(&mut probe)
            .map_err(|source| TsinkError::IoWithPath {
                path: self.root.display_path.join(relative),
                source,
            })?
            != 0
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot required file grew while reading: {}",
                self.root.display_path.join(relative).display()
            )));
        }
        validate_opened_source_file(
            &opened.file,
            &entry.identity,
            entry.len,
            &self.root.display_path.join(relative),
        )?;
        let verification_baseline = checked_add_memory(
            self.retained_memory_bytes,
            bytes.capacity(),
            "secure snapshot measured-file verification",
        )?;
        self.verify_unchanged(verification_baseline)?;
        Ok(bytes)
    }

    /// Copies this measured source below `destination_relative`.
    ///
    /// An empty relative path maps the source root onto the already-created staging root, as used
    /// by restore. A non-empty path creates one destination directory for the source root, as used
    /// by snapshot export's lane/WAL subtrees.
    pub(crate) fn copy_to(
        &self,
        staging: &mut SecureSnapshotStagingDirectory,
        destination_relative: &Path,
    ) -> Result<()> {
        validate_normalized_relative(destination_relative, true)?;
        if !destination_relative.as_os_str().is_empty() {
            staging.create_directory(destination_relative)?;
        }

        for entry in &self.entries {
            let destination = destination_relative.join(&entry.relative);
            match entry.kind {
                SecureSourceKind::Directory => staging.create_directory(&destination)?,
                SecureSourceKind::RegularFile => {
                    let source = open_relative_regular_file(
                        &self.root,
                        &entry.relative,
                        self.parent_identity(&entry.relative)?,
                        &entry.identity,
                    )?;
                    validate_opened_source_file(
                        &source.file,
                        &entry.identity,
                        entry.len,
                        &self.root.display_path.join(&entry.relative),
                    )?;
                    staging.copy_opened_file(
                        source,
                        &destination,
                        entry.len,
                        entry.permissions.clone(),
                        &self.root.display_path.join(&entry.relative),
                    )?;
                }
            }
        }

        self.verify_unchanged(staging.operation_live_retained_bytes()?)?;
        staging.sync_created_directories_below(destination_relative)
    }

    pub(crate) fn verify_unchanged(&self, operation_live_retained_bytes: usize) -> Result<()> {
        let (measurement, entries, manifest_bytes) =
            measure_tree(&self.root, operation_live_retained_bytes)?;
        let current_root_identity = identity_from_file(&self.root.root, &self.root.display_path)?;
        let verification_bytes = source_tree_retained_bytes(&self.root, manifest_bytes)?;
        admit_operation_retained_bytes(
            operation_live_retained_bytes,
            verification_bytes,
            "secure snapshot source verification",
            &self.root.display_path,
        )?;
        if measurement != self.measurement
            || current_root_identity != self.root_identity
            || !source_manifests_match(&self.entries, &entries)
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source namespace changed after secure measurement: {}",
                self.root.display_path.display()
            )));
        }
        Ok(())
    }

    pub(crate) fn verify_requested_namespace_unchanged(&self) -> Result<()> {
        let requested = open_existing_directory_chain_nofollow(
            &self.root.display_path,
            "snapshot source requested-path re-attestation",
        )?;
        let requested_identity = identity_from_file(&requested.root, &self.root.display_path)?;
        let retained_identity = identity_from_file(&self.root.root, &self.root.display_path)?;
        if requested_identity != self.root_identity || retained_identity != self.root_identity {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source requested namespace changed after secure measurement: {}",
                self.root.display_path.display()
            )));
        }
        Ok(())
    }

    fn parent_identity(&self, relative: &Path) -> Result<&ClosedSnapshotIdentity> {
        let parent = relative.parent().unwrap_or(Path::new(""));
        if parent.as_os_str().is_empty() {
            return Ok(&self.root_identity);
        }
        let index = self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(parent))
            .map_err(|_| {
                TsinkError::DataCorruption(format!(
                    "snapshot source file has an unmeasured parent: {}",
                    self.root.display_path.join(relative).display()
                ))
            })?;
        let entry = &self.entries[index];
        if entry.kind != SecureSourceKind::Directory {
            return Err(TsinkError::DataCorruption(format!(
                "snapshot source parent is not a directory: {}",
                self.root.display_path.join(parent).display()
            )));
        }
        Ok(&entry.identity)
    }
}

impl SecureSnapshotSourceFile {
    pub(crate) fn open_with_operation_baseline(
        path: &Path,
        operation_baseline_retained_bytes: usize,
    ) -> Result<Self> {
        let parent_path = path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "snapshot source file has no parent directory: {}",
                path.display()
            ))
        })?;
        let file_name = path.file_name().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "snapshot source file has no final component: {}",
                path.display()
            ))
        })?;
        validate_single_component(file_name, path)?;
        let parent =
            open_existing_directory_chain_nofollow(parent_path, "snapshot file source parent")?;
        let parent_identity = identity_from_file(&parent.root, parent_path)?;
        let relative = Path::new(file_name);
        match probe_relative_entry_kind_nofollow(&parent, relative, &parent_identity)? {
            ProbedEntryKind::RegularFile => {}
            ProbedEntryKind::Directory | ProbedEntryKind::LinkLike | ProbedEntryKind::Other => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "snapshot source is not a plain regular file: {}",
                    path.display()
                )));
            }
        }
        let probe = open_relative_entry_probe(&parent, relative, &parent_identity)?;
        let metadata = probe_metadata(&probe, path)?;
        if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_file() {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source is not a plain regular file: {}",
                path.display()
            )));
        }
        let identity = identity_from_file(&probe.file, path)?;
        let opened = open_relative_regular_file(&parent, relative, &parent_identity, &identity)?;
        validate_opened_source_file(&opened.file, &identity, metadata.len(), path)?;
        drop(opened);
        let mut source = Self {
            parent,
            parent_identity,
            file_name: file_name.to_os_string(),
            display_path: path.to_path_buf(),
            len: metadata.len(),
            permissions: metadata.permissions(),
            identity,
            retained_memory_bytes: 0,
        };
        source.retained_memory_bytes = source_file_retained_bytes(&source)?;
        admit_session_retained_bytes(
            source.retained_memory_bytes,
            "secure snapshot standalone-file session",
            path,
        )?;
        admit_operation_retained_bytes(
            operation_baseline_retained_bytes,
            source.retained_memory_bytes,
            "secure snapshot aggregate source sessions",
            path,
        )?;
        Ok(source)
    }

    pub(crate) fn open_optional_with_operation_baseline(
        path: &Path,
        operation_baseline_retained_bytes: usize,
    ) -> Result<Option<Self>> {
        match Self::open_with_operation_baseline(path, operation_baseline_retained_bytes) {
            Ok(file) => Ok(Some(file)),
            Err(TsinkError::IoWithPath { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn path(&self) -> &Path {
        &self.display_path
    }

    pub(crate) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_bytes
    }

    pub(crate) fn verify_unchanged(&self) -> Result<()> {
        self.attest_current_identity()
    }

    pub(crate) fn read_all_bounded(&self, max_len: usize) -> Result<Vec<u8>> {
        let length = usize::try_from(self.len).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "snapshot source file length exceeds this platform's addressable range: {}",
                self.display_path.display()
            ))
        })?;
        if length > max_len {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source file uses {length} bytes, exceeding limit {max_len}: {}",
                self.display_path.display()
            )));
        }
        let relative = Path::new(&self.file_name);
        let mut opened = open_relative_regular_file(
            &self.parent,
            relative,
            &self.parent_identity,
            &self.identity,
        )?;
        validate_opened_source_file(&opened.file, &self.identity, self.len, &self.display_path)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| {
            TsinkError::Other(format!(
                "unable to allocate snapshot source file buffer: {}",
                self.display_path.display()
            ))
        })?;
        (&mut opened.file)
            .take(self.len)
            .read_to_end(&mut bytes)
            .map_err(|source| TsinkError::IoWithPath {
                path: self.display_path.clone(),
                source,
            })?;
        if bytes.len() != length {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source shrank while reading: {} yielded {} of {} bytes",
                self.display_path.display(),
                bytes.len(),
                self.len
            )));
        }
        let mut probe = [0u8; 1];
        if opened
            .file
            .read(&mut probe)
            .map_err(|source| TsinkError::IoWithPath {
                path: self.display_path.clone(),
                source,
            })?
            != 0
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source grew while reading: {}",
                self.display_path.display()
            )));
        }
        validate_opened_source_file(&opened.file, &self.identity, self.len, &self.display_path)?;
        self.attest_current_identity()?;
        Ok(bytes)
    }

    pub(crate) fn copy_to(
        &self,
        staging: &mut SecureSnapshotStagingDirectory,
        destination_relative: &Path,
    ) -> Result<()> {
        let relative = Path::new(&self.file_name);
        let opened = open_relative_regular_file(
            &self.parent,
            relative,
            &self.parent_identity,
            &self.identity,
        )?;
        validate_opened_source_file(&opened.file, &self.identity, self.len, &self.display_path)?;
        staging.copy_opened_file(
            opened,
            destination_relative,
            self.len,
            self.permissions.clone(),
            &self.display_path,
        )?;
        self.attest_current_identity()
    }

    fn attest_current_identity(&self) -> Result<()> {
        let probe = open_relative_entry_probe(
            &self.parent,
            Path::new(&self.file_name),
            &self.parent_identity,
        )?;
        let metadata = probe_metadata(&probe, &self.display_path)?;
        let actual = identity_from_file(&probe.file, &self.display_path)?;
        if is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
            || metadata.len() != self.len
            || actual != self.identity
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source path changed after secure open: {}",
                self.display_path.display()
            )));
        }
        Ok(())
    }
}

impl SecureSnapshotNamespaceFence {
    pub(crate) fn open_with_operation_baseline(
        path: &Path,
        operation_baseline_retained_bytes: usize,
    ) -> Result<Self> {
        let anchor =
            open_existing_directory_chain_nofollow(path, "snapshot aggregate namespace fence")?;
        let identity = identity_from_file(&anchor.root, path)?;
        let retained_memory_bytes = checked_add_memory(
            std::mem::size_of::<Self>(),
            anchor_dynamic_retained_bytes(&anchor)?,
            "snapshot aggregate namespace fence",
        )?;
        admit_operation_retained_bytes(
            operation_baseline_retained_bytes,
            retained_memory_bytes,
            "snapshot aggregate namespace fence",
            path,
        )?;
        Ok(Self {
            anchor,
            identity,
            retained_memory_bytes,
        })
    }

    pub(crate) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_bytes
    }

    pub(crate) fn attest(&self) -> Result<()> {
        let requested = open_existing_directory_chain_nofollow(
            &self.anchor.display_path,
            "snapshot aggregate namespace re-attestation",
        )?;
        let requested_identity = identity_from_file(&requested.root, &self.anchor.display_path)?;
        let anchored_identity = identity_from_file(&self.anchor.root, &self.anchor.display_path)?;
        if anchored_identity != self.identity || requested_identity != self.identity {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot aggregate source namespace changed between component measurements: {}",
                self.anchor.display_path.display()
            )));
        }
        Ok(())
    }
}

impl SecureSnapshotPublishedBackup {
    pub(crate) fn remove_and_sync_parent(self) -> Result<()> {
        // Bind the retained pre-move root handle to the current backup name before touching any
        // descendant. If the original was renamed away and the backup name was replaced, cleanup
        // must not follow the retained handle and delete the displaced original elsewhere.
        attest_root_entry(
            &self.parent,
            &self.backup_name,
            &self.backup_path,
            &self.root_identity,
        )?;
        let verification_baseline = checked_add_memory(
            self.external_operation_baseline_bytes,
            self.retained_memory_bytes,
            "secure restore backup cleanup verification",
        )?;
        let (measurement, entries, manifest_bytes) =
            measure_tree(&self.root, verification_baseline)?;
        let verification_bytes = source_tree_retained_bytes(&self.root, manifest_bytes)?;
        admit_operation_retained_bytes(
            verification_baseline,
            verification_bytes,
            "secure restore backup cleanup verification",
            &self.backup_path,
        )?;
        let current_root_identity = identity_from_file(&self.root.root, &self.backup_path)?;
        if measurement != self.measurement
            || !current_root_identity.stable_eq(self.root_identity)
            || !cleanup_source_manifests_match(&self.entries, &entries)
        {
            return Err(TsinkError::DataCorruption(format!(
                "restore backup changed after its pre-move identity manifest was captured: {}",
                self.backup_path.display()
            )));
        }
        remove_published_backup_exact(self)
    }

    fn parent_identity(&self, relative: &Path) -> Result<&ClosedSnapshotIdentity> {
        let parent = relative.parent().unwrap_or(Path::new(""));
        if parent.as_os_str().is_empty() {
            return Ok(&self.root_identity);
        }
        let index = self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(parent))
            .map_err(|_| {
                TsinkError::DataCorruption(format!(
                    "restore backup file has an unmeasured parent: {}",
                    self.backup_path.join(relative).display()
                ))
            })?;
        let entry = &self.entries[index];
        if entry.kind != SecureSourceKind::Directory {
            return Err(TsinkError::DataCorruption(format!(
                "restore backup parent is not a directory: {}",
                self.backup_path.join(parent).display()
            )));
        }
        Ok(&entry.identity)
    }
}

impl SecureSnapshotStagingDirectory {
    pub(crate) fn create_unique(target: &Path, purpose: &str) -> Result<Self> {
        Self::create_unique_inner(target, purpose, true)
    }

    /// Creates a unique staging sibling while allowing the eventual target to exist.
    ///
    /// Restore uses this constructor before it decides whether activation is a no-replace publish
    /// or an identity-attested target-to-backup replacement.
    pub(crate) fn create_unique_replacement_sibling(target: &Path, purpose: &str) -> Result<Self> {
        Self::create_unique_inner(target, purpose, false)
    }

    fn create_unique_inner(
        target: &Path,
        purpose: &str,
        require_absent_target: bool,
    ) -> Result<Self> {
        let parent_path = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "{purpose} target has no parent directory: {}",
                target.display()
            ))
        })?;
        let parent = create_directory_chain_nofollow(parent_path, purpose)?;
        let target_name = target
            .file_name()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| OsStr::new("snapshot"));
        validate_single_component(target_name, target)?;
        if require_absent_target && relative_entry_exists_nofollow(&parent, Path::new(target_name))?
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{purpose} destination already exists: {}",
                target.display()
            )));
        }
        for _ in 0..256 {
            let candidate = super::next_stage_dir_candidate(target, purpose)?;
            let Some(name) = candidate.file_name().map(OsStr::to_os_string) else {
                continue;
            };
            match create_staging_at_parent(&parent, &name, &candidate) {
                Ok(root) => {
                    return Self::from_created(parent, root, candidate, name);
                }
                Err(TsinkError::IoWithPath { source, .. })
                    if source.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
        Err(TsinkError::Other(format!(
            "failed to securely create a unique staging directory for {}",
            target.display()
        )))
    }

    pub(crate) fn create_exact(path: &Path, operation: &str) -> Result<Self> {
        let parent_path = path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "{operation} staging has no parent directory: {}",
                path.display()
            ))
        })?;
        let root_name = path.file_name().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "{operation} staging has no final component: {}",
                path.display()
            ))
        })?;
        validate_single_component(root_name, path)?;
        let parent = open_existing_directory_chain_nofollow(parent_path, operation)?;
        let root = create_staging_at_parent(&parent, root_name, path)?;
        Self::from_created(parent, root, path.to_path_buf(), root_name.to_os_string())
    }

    fn from_created(
        parent: SecureDirectoryAnchor,
        root: SecureDirectoryAnchor,
        _requested_display_path: PathBuf,
        root_name: OsString,
    ) -> Result<Self> {
        // `root.display_path` is the path resolved beneath the retained parent anchor. On Windows
        // this is deliberately absolute, so a later current-directory change cannot redirect the
        // narrow root-handle release immediately before publication.
        let display_path = root.display_path.clone();
        let identity = identity_from_file(&root.root, &display_path)?;
        attest_root_entry(&parent, &root_name, &display_path, &identity)?;
        let created = vec![SecureCreatedEntry {
            relative: PathBuf::new(),
            kind: SecureSourceKind::Directory,
            identity,
        }];
        let created_manifest_bytes = created
            .capacity()
            .checked_mul(std::mem::size_of::<SecureCreatedEntry>())
            .ok_or_else(|| {
                TsinkError::Other(
                    "secure snapshot staging manifest capacity accounting overflow".to_string(),
                )
            })?;
        let mut created_directory_index = HashMap::new();
        created_directory_index.try_reserve(1).map_err(|_| {
            TsinkError::Other("unable to allocate secure staging directory index".to_string())
        })?;
        created_directory_index.insert(relative_path_hash(Path::new("")), 0);
        let created_directory_index_bytes =
            modeled_directory_index_bytes(&created_directory_index)?;
        let mut staging = Self {
            parent,
            root,
            display_path,
            root_name,
            created,
            created_directory_index,
            created_directory_index_bytes,
            created_manifest_bytes,
            retained_memory_bytes: 0,
            operation_baseline_retained_bytes: 0,
        };
        staging.refresh_retained_memory()?;
        Ok(staging)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.display_path
    }

    pub(crate) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_bytes
    }

    /// Records every other simultaneously live secure session or generated buffer.
    ///
    /// Staging growth and both source/staging verification passes include this baseline in the
    /// single operation-wide memory ceiling.
    pub(crate) fn set_operation_baseline_retained_bytes(
        &mut self,
        bytes: usize,
        operation: &str,
    ) -> Result<()> {
        admit_operation_retained_bytes(
            bytes,
            self.retained_memory_bytes,
            operation,
            &self.display_path,
        )?;
        self.operation_baseline_retained_bytes = bytes;
        Ok(())
    }

    pub(crate) fn write_file(
        &mut self,
        relative: &Path,
        bytes: &[u8],
        permissions: Option<Permissions>,
    ) -> Result<()> {
        validate_normalized_relative(relative, false)?;
        let display = self.display_path.join(relative);
        let parent_identity = *self.created_parent_identity(relative)?;
        let (mut destination, parent) =
            create_relative_regular_file(&self.root, relative, &display, &parent_identity)?;
        destination
            .file
            .write_all(bytes)
            .and_then(|_| destination.file.flush())
            .map_err(|source| TsinkError::IoWithPath {
                path: display.clone(),
                source,
            })?;
        if let Some(permissions) = permissions {
            destination
                .file
                .set_permissions(permissions)
                .map_err(|source| TsinkError::IoWithPath {
                    path: display.clone(),
                    source,
                })?;
        }
        #[cfg(test)]
        invoke_file_sync_hook(&display)?;
        destination
            .file
            .sync_all()
            .map_err(|source| TsinkError::IoWithPath {
                path: display.clone(),
                source,
            })?;
        let identity = identity_from_file(&destination.file, &display)?;
        attest_relative_identity(
            &self.root,
            relative,
            &display,
            SecureSourceKind::RegularFile,
            &parent_identity,
            &identity,
        )?;
        sync_opened_directory(&parent, display.parent().unwrap_or(&self.display_path))?;
        self.record_created(relative, SecureSourceKind::RegularFile, identity)
    }

    pub(crate) fn ensure_declared_regular_file(
        &mut self,
        relative: &Path,
        initial_bytes: &[u8],
    ) -> Result<()> {
        validate_normalized_relative(relative, false)?;
        if let Some(existing) = self.created.iter().find(|entry| entry.relative == relative) {
            return if existing.kind == SecureSourceKind::RegularFile {
                Ok(())
            } else {
                Err(TsinkError::InvalidConfiguration(format!(
                    "restore validation requires a regular file at {}",
                    self.display_path.join(relative).display()
                )))
            };
        }
        self.write_file(relative, initial_bytes, None)
    }

    pub(crate) fn verify_exact_created_tree(&self) -> Result<()> {
        attest_root_entry(
            &self.parent,
            &self.root_name,
            &self.display_path,
            &self.created[0].identity,
        )?;
        let live_retained = self.operation_live_retained_bytes()?;
        let (observed, observed_bytes) = measure_created_tree(&self.root, live_retained)?;
        admit_operation_retained_bytes(
            live_retained,
            observed_bytes,
            "secure snapshot staging verification",
            &self.display_path,
        )?;
        if !created_manifests_match(&self.created, &observed) {
            return Err(TsinkError::DataCorruption(format!(
                "snapshot staging namespace contains a missing, unknown, or replaced entry: {}",
                self.display_path.display()
            )));
        }
        Ok(())
    }

    pub(crate) fn sync_root(&self) -> Result<()> {
        self.verify_exact_created_tree()?;
        sync_file_as_directory(&self.root.root, &self.display_path)
    }

    /// Removes a validation copy after a real storage open has been allowed to rewrite known
    /// files.
    ///
    /// The original create-exclusive manifest remains the ownership boundary. This method accepts
    /// refreshed identities only when the post-validation tree has exactly the same relative
    /// paths and entry kinds: it never discovers or adopts a new path, and it refuses cleanup if a
    /// known path disappeared. The refreshed exact manifest is then re-attested during
    /// handle-relative bottom-up deletion.
    pub(crate) fn remove_after_exact_manifest_refresh(mut self) -> Result<()> {
        attest_root_entry(
            &self.parent,
            &self.root_name,
            &self.display_path,
            &self.created[0].identity,
        )?;
        let live_retained = self.operation_live_retained_bytes()?;
        let (measurement, entries, manifest_bytes) = measure_tree(&self.root, live_retained)?;
        let refreshed_retained = source_tree_retained_bytes(&self.root, manifest_bytes)?;
        admit_operation_retained_bytes(
            live_retained,
            refreshed_retained,
            "secure restore validation-copy cleanup refresh",
            &self.display_path,
        )?;
        let current_root_identity = identity_from_file(&self.root.root, &self.display_path)?;
        if !current_root_identity.stable_eq(self.created[0].identity) {
            return Err(TsinkError::DataCorruption(format!(
                "restore validation staging root changed before cleanup: {}",
                self.display_path.display()
            )));
        }
        self.created
            .sort_unstable_by(|left, right| left.relative.cmp(&right.relative));
        if !created_and_source_manifest_shapes_match(&self.created, &entries) {
            return Err(TsinkError::DataCorruption(format!(
                "restore validation staging contains a missing, unknown, or type-changed entry after production open; refusing cleanup outside the original manifest: {}",
                self.display_path.display()
            )));
        }

        let SecureSnapshotStagingDirectory {
            parent,
            root,
            display_path,
            root_name,
            operation_baseline_retained_bytes,
            ..
        } = self;
        let mut cleanup = SecureSnapshotPublishedBackup {
            parent,
            root,
            requested_backup_path: display_path.clone(),
            backup_path: display_path,
            backup_name: root_name,
            root_identity: current_root_identity,
            measurement,
            entries,
            retained_memory_bytes: 0,
            external_operation_baseline_bytes: operation_baseline_retained_bytes,
        };
        cleanup.retained_memory_bytes = published_backup_retained_bytes(&cleanup)?;
        admit_operation_retained_bytes(
            cleanup.external_operation_baseline_bytes,
            cleanup.retained_memory_bytes,
            "secure restore validation-copy cleanup token",
            &cleanup.backup_path,
        )?;
        cleanup.remove_and_sync_parent()
    }

    /// Publishes this staging root to an absent sibling through the retained parent anchor.
    ///
    /// The session is consumed so Windows can release the staging directory's no-delete handle
    /// immediately before `MoveFileExW` while retaining every parent/ancestor lock. Unix renames
    /// both names relative to the already-open parent descriptor.
    pub(crate) fn publish_noreplace(
        self,
        target: &Path,
    ) -> std::result::Result<(), SecureSnapshotPublicationError> {
        self.verify_exact_created_tree()
            .map_err(SecureSnapshotPublicationError::before_publication)?;
        let target_parent = target.parent().ok_or_else(|| {
            SecureSnapshotPublicationError::before_publication(TsinkError::InvalidConfiguration(
                format!(
                    "snapshot publication target has no parent: {}",
                    target.display()
                ),
            ))
        })?;
        let target_parent_anchor =
            open_existing_directory_chain_nofollow(target_parent, "snapshot publication parent")
                .map_err(SecureSnapshotPublicationError::before_publication)?;
        let retained_parent_identity =
            identity_from_file(&self.parent.root, &self.parent.display_path)
                .map_err(SecureSnapshotPublicationError::before_publication)?;
        let requested_parent_identity =
            identity_from_file(&target_parent_anchor.root, target_parent)
                .map_err(SecureSnapshotPublicationError::before_publication)?;
        if !retained_parent_identity.stable_eq(requested_parent_identity) {
            return Err(SecureSnapshotPublicationError::before_publication(
                TsinkError::InvalidConfiguration(format!(
                    "secure snapshot publication target parent {} is not the retained parent {}",
                    target_parent.display(),
                    self.parent.display_path.display()
                )),
            ));
        }
        let target_name = target.file_name().ok_or_else(|| {
            SecureSnapshotPublicationError::before_publication(TsinkError::InvalidConfiguration(
                format!(
                    "snapshot publication target has no final component: {}",
                    target.display()
                ),
            ))
        })?;
        validate_single_component(target_name, target)
            .map_err(SecureSnapshotPublicationError::before_publication)?;
        let anchored_target = self.parent.display_path.join(target_name);
        publish_staging_noreplace(self, target, &anchored_target, target_name)
    }

    /// Replaces one existing sibling by first moving it to an absent backup sibling.
    ///
    /// Every transition is relative to the staging session's retained parent. Failures before the
    /// staging root becomes the target attempt an identity-attested backup rollback and retain the
    /// staging tree. Once staging is visible at `target`, failures never roll it back or delete it.
    pub(crate) fn publish_replacing(
        self,
        target: &Path,
        backup: &Path,
        original_target: SecureSnapshotSourceTree,
    ) -> std::result::Result<SecureSnapshotPublishedBackup, SecureSnapshotPublicationError> {
        self.verify_exact_created_tree()
            .map_err(SecureSnapshotPublicationError::before_publication)?;
        let target_name = self
            .validate_publication_sibling(target, "restore replacement target")
            .map_err(SecureSnapshotPublicationError::before_publication)?;
        let backup_name = self
            .validate_publication_sibling(backup, "restore replacement backup")
            .map_err(SecureSnapshotPublicationError::before_publication)?;
        if target_name == backup_name
            || target_name == self.root_name
            || backup_name == self.root_name
        {
            return Err(SecureSnapshotPublicationError::before_publication(
                TsinkError::InvalidConfiguration(format!(
                    "restore target, staging, and backup names must be distinct siblings: {}, {}, {}",
                    target.display(),
                    self.display_path.display(),
                    backup.display()
                )),
            ));
        }
        let anchored_target = self.parent.display_path.join(&target_name);
        let anchored_backup = self.parent.display_path.join(&backup_name);
        let external_operation_baseline_bytes = self
            .operation_baseline_retained_bytes
            .saturating_sub(original_target.retained_memory_bytes());
        let publication_live_retained = admit_secure_snapshot_operation_retained_bytes(
            &[
                external_operation_baseline_bytes,
                original_target.retained_memory_bytes(),
                self.retained_memory_bytes(),
            ],
            "secure restore replacement publication state",
            target,
        )
        .map_err(SecureSnapshotPublicationError::before_publication)?;
        original_target
            .verify_unchanged(publication_live_retained)
            .map_err(SecureSnapshotPublicationError::before_publication)?;
        let paths = SecureSnapshotReplacementPaths {
            requested_target: target,
            target: &anchored_target,
            target_name: &target_name,
            requested_backup: backup,
            backup: &anchored_backup,
            backup_name: &backup_name,
        };
        publish_staging_replacing(
            self,
            paths,
            original_target,
            external_operation_baseline_bytes,
        )
    }

    fn validate_publication_sibling(&self, path: &Path, operation: &str) -> Result<OsString> {
        let parent_path = path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "{operation} has no parent directory: {}",
                path.display()
            ))
        })?;
        let requested_parent = open_existing_directory_chain_nofollow(parent_path, operation)?;
        let retained_identity = identity_from_file(&self.parent.root, &self.parent.display_path)?;
        let requested_identity = identity_from_file(&requested_parent.root, parent_path)?;
        if !retained_identity.stable_eq(requested_identity) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{operation} parent {} is not retained staging parent {}",
                parent_path.display(),
                self.parent.display_path.display()
            )));
        }
        let name = path.file_name().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "{operation} has no final component: {}",
                path.display()
            ))
        })?;
        validate_single_component(name, path)?;
        Ok(name.to_os_string())
    }

    fn create_directory(&mut self, relative: &Path) -> Result<()> {
        validate_normalized_relative(relative, false)?;
        let display = self.display_path.join(relative);
        let parent_identity = *self.created_parent_identity(relative)?;
        let (opened, parent) =
            create_relative_directory(&self.root, relative, &display, &parent_identity)?;
        let identity = identity_from_file(&opened.file, &display)?;
        attest_relative_identity(
            &self.root,
            relative,
            &display,
            SecureSourceKind::Directory,
            &parent_identity,
            &identity,
        )?;
        sync_opened_directory(&parent, display.parent().unwrap_or(&self.display_path))?;
        self.record_created(relative, SecureSourceKind::Directory, identity)
    }

    fn copy_opened_file(
        &mut self,
        mut source: SecureOpenedFile,
        relative: &Path,
        expected_len: u64,
        permissions: Permissions,
        source_display: &Path,
    ) -> Result<()> {
        validate_normalized_relative(relative, false)?;
        let display = self.display_path.join(relative);
        let parent_identity = *self.created_parent_identity(relative)?;
        let (mut destination, parent) =
            create_relative_regular_file(&self.root, relative, &display, &parent_identity)?;
        let copied = {
            let mut bounded = (&mut source.file).take(expected_len);
            std::io::copy(&mut bounded, &mut destination.file).map_err(|source| {
                TsinkError::IoWithPath {
                    path: display.clone(),
                    source,
                }
            })?
        };
        if copied != expected_len {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source shrank while copying: {} yielded {copied} of {expected_len} bytes",
                source_display.display()
            )));
        }
        let mut extra = [0u8; 1];
        if source
            .file
            .read(&mut extra)
            .map_err(|source| TsinkError::IoWithPath {
                path: source_display.to_path_buf(),
                source,
            })?
            != 0
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot source grew while copying: {}",
                source_display.display()
            )));
        }
        // `File::set_permissions` mutates the already-open destination handle. Never use the
        // destination pathname after creation for permission changes.
        destination
            .file
            .set_permissions(permissions)
            .and_then(|_| destination.file.flush())
            .map_err(|source| TsinkError::IoWithPath {
                path: display.clone(),
                source,
            })?;
        #[cfg(test)]
        invoke_file_sync_hook(&display)?;
        destination
            .file
            .sync_all()
            .map_err(|source| TsinkError::IoWithPath {
                path: display.clone(),
                source,
            })?;
        let identity = identity_from_file(&destination.file, &display)?;
        attest_relative_identity(
            &self.root,
            relative,
            &display,
            SecureSourceKind::RegularFile,
            &parent_identity,
            &identity,
        )?;
        sync_opened_directory(&parent, display.parent().unwrap_or(&self.display_path))?;
        self.record_created(relative, SecureSourceKind::RegularFile, identity)
    }

    fn record_created(
        &mut self,
        relative: &Path,
        kind: SecureSourceKind,
        identity: ClosedSnapshotIdentity,
    ) -> Result<()> {
        // The directory/file creation primitive is create-exclusive, so a duplicate relative path
        // fails before this point without an O(n) manifest scan.
        validate_retained_relative_path(relative)?;
        let retained_path = relative.to_path_buf();
        let path_capacity = retained_path.capacity();
        let old_capacity = self.created.capacity();
        self.created
            .try_reserve(1)
            .map_err(|_| TsinkError::Other("unable to retain staging identity".to_string()))?;
        let capacity_growth = self
            .created
            .capacity()
            .checked_sub(old_capacity)
            .and_then(|growth| growth.checked_mul(std::mem::size_of::<SecureCreatedEntry>()))
            .ok_or_else(|| {
                TsinkError::Other(
                    "secure snapshot staging manifest capacity accounting overflow".to_string(),
                )
            })?;
        let prospective_manifest = checked_add_memory(
            checked_add_memory(
                self.created_manifest_bytes,
                capacity_growth,
                "secure snapshot staging manifest",
            )?,
            path_capacity,
            "secure snapshot staging manifest",
        )?;
        let prospective_retained = staging_retained_bytes(self, prospective_manifest)?;
        admit_session_retained_bytes(
            prospective_retained,
            "secure snapshot staging identity manifest",
            &self.display_path,
        )?;
        self.created.push(SecureCreatedEntry {
            relative: retained_path,
            kind,
            identity,
        });
        self.created_manifest_bytes = prospective_manifest;
        if kind == SecureSourceKind::Directory {
            let index = self.created.len() - 1;
            let hash = relative_path_hash(&self.created[index].relative);
            if let Some(existing) = self.created_directory_index.get(&hash).copied() {
                if self.created[existing].relative != self.created[index].relative {
                    return Err(TsinkError::DataCorruption(format!(
                        "secure snapshot directory-index hash collision between {} and {}",
                        self.created[existing].relative.display(),
                        self.created[index].relative.display()
                    )));
                }
                return Err(TsinkError::DataCorruption(format!(
                    "secure snapshot directory was recorded twice: {}",
                    self.created[index].relative.display()
                )));
            }
            self.created_directory_index.try_reserve(1).map_err(|_| {
                TsinkError::Other("unable to extend secure staging directory index".to_string())
            })?;
            self.created_directory_index.insert(hash, index);
            self.created_directory_index_bytes =
                modeled_directory_index_bytes(&self.created_directory_index)?;
        }
        self.refresh_retained_memory()?;
        Ok(())
    }

    fn created_parent_identity(&self, relative: &Path) -> Result<&ClosedSnapshotIdentity> {
        let parent = relative.parent().unwrap_or(Path::new(""));
        let hash = relative_path_hash(parent);
        self.created_directory_index
            .get(&hash)
            .and_then(|index| self.created.get(*index))
            .filter(|entry| entry.kind == SecureSourceKind::Directory && entry.relative == parent)
            .map(|entry| &entry.identity)
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "snapshot staging path has an unrecorded parent: {}",
                    self.display_path.join(relative).display()
                ))
            })
    }

    fn refresh_retained_memory(&mut self) -> Result<()> {
        let retained = staging_retained_bytes(self, self.created_manifest_bytes)?;
        admit_session_retained_bytes(
            retained,
            "secure snapshot staging identity manifest",
            &self.display_path,
        )?;
        admit_operation_retained_bytes(
            self.operation_baseline_retained_bytes,
            retained,
            "secure snapshot aggregate operation state",
            &self.display_path,
        )?;
        self.retained_memory_bytes = retained;
        Ok(())
    }

    fn operation_live_retained_bytes(&self) -> Result<usize> {
        checked_add_memory(
            self.operation_baseline_retained_bytes,
            self.retained_memory_bytes,
            "secure snapshot aggregate operation state",
        )
    }

    fn sync_created_directories_below(&self, prefix: &Path) -> Result<()> {
        // Directories are recorded at create time before any descendants. Reverse iteration is
        // therefore already child-before-parent and needs no transient Vec proportional to the
        // admitted namespace.
        for entry in self.created.iter().rev().filter(|entry| {
            entry.kind == SecureSourceKind::Directory
                && entry.relative.starts_with(prefix)
                && !entry.relative.as_os_str().is_empty()
        }) {
            let opened = open_relative_directory(&self.root, &entry.relative, &entry.identity)?;
            sync_opened_directory(&opened, &self.display_path.join(&entry.relative))?;
        }
        Ok(())
    }
}

fn attest_requested_publication_parent(
    retained_parent: &SecureDirectoryAnchor,
    requested_path: &Path,
    operation: &str,
) -> Result<()> {
    let requested_parent_path = requested_path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "{operation} path has no parent: {}",
            requested_path.display()
        ))
    })?;
    let requested_parent =
        open_existing_directory_chain_nofollow(requested_parent_path, operation)?;
    let retained_identity =
        identity_from_file(&retained_parent.root, &retained_parent.display_path)?;
    let requested_identity = identity_from_file(&requested_parent.root, requested_parent_path)?;
    if !retained_identity.stable_eq(requested_identity) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} completed in retained parent {}, but requested parent {} was replaced or renamed before post-publication attestation",
            retained_parent.display_path.display(),
            requested_parent_path.display()
        )));
    }
    Ok(())
}

impl SecureSnapshotPublicationError {
    fn before_publication(error: TsinkError) -> Self {
        Self {
            error,
            published: false,
        }
    }

    fn after_publication(error: TsinkError) -> Self {
        Self {
            error,
            published: true,
        }
    }
}

fn validate_retained_relative_path(path: &Path) -> Result<()> {
    let bytes = path.as_os_str().as_encoded_bytes().len();
    if bytes > MAX_SECURE_SNAPSHOT_RELATIVE_PATH_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "secure snapshot relative path uses {bytes} bytes, exceeding limit {}: {}",
            MAX_SECURE_SNAPSHOT_RELATIVE_PATH_BYTES,
            path.display()
        )));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ManifestMemoryTracker {
    retained_bytes: usize,
    operation_baseline_bytes: usize,
}

impl ManifestMemoryTracker {
    fn push_source(
        &mut self,
        entries: &mut Vec<SecureSourceEntry>,
        entry: SecureSourceEntry,
    ) -> Result<()> {
        self.push(
            entries,
            entry,
            |entry| (entry.relative.as_path(), entry.relative.capacity()),
            "secure snapshot source manifest",
        )
    }

    fn push_created(
        &mut self,
        entries: &mut Vec<SecureCreatedEntry>,
        entry: SecureCreatedEntry,
    ) -> Result<()> {
        self.push(
            entries,
            entry,
            |entry| (entry.relative.as_path(), entry.relative.capacity()),
            "secure snapshot staging manifest",
        )
    }

    fn push<T>(
        &mut self,
        entries: &mut Vec<T>,
        entry: T,
        path_of: impl Fn(&T) -> (&Path, usize),
        operation: &str,
    ) -> Result<()> {
        let (path, path_capacity) = path_of(&entry);
        validate_retained_relative_path(path)?;
        let old_capacity = entries.capacity();
        entries
            .try_reserve(1)
            .map_err(|_| TsinkError::Other(format!("unable to retain {operation} entry")))?;
        let capacity_growth = entries
            .capacity()
            .checked_sub(old_capacity)
            .and_then(|growth| growth.checked_mul(std::mem::size_of::<T>()))
            .ok_or_else(|| {
                TsinkError::Other(format!("{operation} capacity accounting overflow"))
            })?;
        let prospective = checked_add_memory(
            checked_add_memory(self.retained_bytes, capacity_growth, operation)?,
            path_capacity,
            operation,
        )?;
        admit_session_retained_bytes(prospective, operation, path)?;
        admit_operation_retained_bytes(
            self.operation_baseline_bytes,
            prospective,
            operation,
            path,
        )?;
        entries.push(entry);
        self.retained_bytes = prospective;
        Ok(())
    }
}

fn checked_add_memory(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        TsinkError::Other(format!("{operation} retained-memory accounting overflow"))
    })
}

fn anchor_dynamic_retained_bytes(anchor: &SecureDirectoryAnchor) -> Result<usize> {
    let bytes = anchor.display_path.capacity();
    #[cfg(windows)]
    let bytes = checked_add_memory(
        bytes,
        anchor
            .ancestor_locks
            .capacity()
            .checked_mul(std::mem::size_of::<File>())
            .ok_or_else(|| {
                TsinkError::Other(
                    "secure snapshot Windows ancestor-lock accounting overflow".to_string(),
                )
            })?,
        "secure snapshot directory anchor",
    )?;
    Ok(bytes)
}

fn source_tree_retained_bytes(
    root: &SecureDirectoryAnchor,
    manifest_bytes: usize,
) -> Result<usize> {
    let bytes = checked_add_memory(
        std::mem::size_of::<SecureSnapshotSourceTree>(),
        anchor_dynamic_retained_bytes(root)?,
        "secure snapshot source session",
    )?;
    checked_add_memory(bytes, manifest_bytes, "secure snapshot source session")
}

fn source_file_retained_bytes(source: &SecureSnapshotSourceFile) -> Result<usize> {
    let mut bytes = std::mem::size_of::<SecureSnapshotSourceFile>();
    bytes = checked_add_memory(
        bytes,
        anchor_dynamic_retained_bytes(&source.parent)?,
        "secure snapshot standalone-file session",
    )?;
    bytes = checked_add_memory(
        bytes,
        source.file_name.capacity(),
        "secure snapshot standalone-file session",
    )?;
    checked_add_memory(
        bytes,
        source.display_path.capacity(),
        "secure snapshot standalone-file session",
    )
}

fn staging_retained_bytes(
    staging: &SecureSnapshotStagingDirectory,
    manifest_bytes: usize,
) -> Result<usize> {
    let mut bytes = std::mem::size_of::<SecureSnapshotStagingDirectory>();
    bytes = checked_add_memory(
        bytes,
        anchor_dynamic_retained_bytes(&staging.parent)?,
        "secure snapshot staging session",
    )?;
    bytes = checked_add_memory(
        bytes,
        anchor_dynamic_retained_bytes(&staging.root)?,
        "secure snapshot staging session",
    )?;
    bytes = checked_add_memory(
        bytes,
        staging.display_path.capacity(),
        "secure snapshot staging session",
    )?;
    bytes = checked_add_memory(
        bytes,
        staging.root_name.capacity(),
        "secure snapshot staging session",
    )?;
    bytes = checked_add_memory(
        bytes,
        staging.created_directory_index_bytes,
        "secure snapshot staging session",
    )?;
    checked_add_memory(bytes, manifest_bytes, "secure snapshot staging session")
}

fn relative_path_hash(path: &Path) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

fn modeled_directory_index_bytes(index: &HashMap<u64, usize>) -> Result<usize> {
    index
        .capacity()
        .checked_mul(std::mem::size_of::<(u64, usize)>() + 1)
        .ok_or_else(|| {
            TsinkError::Other(
                "secure snapshot directory-index memory accounting overflow".to_string(),
            )
        })
}

fn source_manifest_modeled_bytes(entries: &Vec<SecureSourceEntry>) -> Result<usize> {
    let mut bytes = entries
        .capacity()
        .checked_mul(std::mem::size_of::<SecureSourceEntry>())
        .ok_or_else(|| {
            TsinkError::Other("secure snapshot source-manifest accounting overflow".to_string())
        })?;
    for entry in entries {
        bytes = checked_add_memory(
            bytes,
            entry.relative.capacity(),
            "secure snapshot source manifest",
        )?;
    }
    Ok(bytes)
}

fn published_backup_retained_bytes(backup: &SecureSnapshotPublishedBackup) -> Result<usize> {
    let mut bytes = std::mem::size_of::<SecureSnapshotPublishedBackup>();
    bytes = checked_add_memory(
        bytes,
        anchor_dynamic_retained_bytes(&backup.parent)?,
        "secure restore backup cleanup token",
    )?;
    bytes = checked_add_memory(
        bytes,
        anchor_dynamic_retained_bytes(&backup.root)?,
        "secure restore backup cleanup token",
    )?;
    bytes = checked_add_memory(
        bytes,
        backup.requested_backup_path.capacity(),
        "secure restore backup cleanup token",
    )?;
    bytes = checked_add_memory(
        bytes,
        backup.backup_path.capacity(),
        "secure restore backup cleanup token",
    )?;
    bytes = checked_add_memory(
        bytes,
        backup.backup_name.capacity(),
        "secure restore backup cleanup token",
    )?;
    checked_add_memory(
        bytes,
        source_manifest_modeled_bytes(&backup.entries)?,
        "secure restore backup cleanup token",
    )
}

fn admit_session_retained_bytes(bytes: usize, operation: &str, path: &Path) -> Result<()> {
    if bytes > MAX_SECURE_SNAPSHOT_SESSION_RETAINED_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} requires {bytes} retained bytes, exceeding limit {} at {}",
            MAX_SECURE_SNAPSHOT_SESSION_RETAINED_BYTES,
            path.display()
        )));
    }
    Ok(())
}

fn admit_operation_retained_bytes(
    retained: usize,
    transient: usize,
    operation: &str,
    path: &Path,
) -> Result<()> {
    let total = checked_add_memory(retained, transient, operation)?;
    if total > MAX_SECURE_SNAPSHOT_OPERATION_RETAINED_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} requires {total} retained-plus-transient bytes, exceeding limit {} at {}",
            MAX_SECURE_SNAPSHOT_OPERATION_RETAINED_BYTES,
            path.display()
        )));
    }
    Ok(())
}

pub(crate) fn admit_secure_snapshot_operation_retained_bytes(
    parts: &[usize],
    operation: &str,
    path: &Path,
) -> Result<usize> {
    let total = parts.iter().try_fold(0usize, |total, part| {
        checked_add_memory(total, *part, operation)
    })?;
    admit_operation_retained_bytes(total, 0, operation, path)?;
    Ok(total)
}

fn source_manifests_match(expected: &[SecureSourceEntry], actual: &[SecureSourceEntry]) -> bool {
    expected.len() == actual.len()
        && expected.iter().zip(actual).all(|(left, right)| {
            left.relative == right.relative
                && left.kind == right.kind
                && left.len == right.len
                && left.identity == right.identity
        })
}

fn cleanup_source_manifests_match(
    expected: &[SecureSourceEntry],
    actual: &[SecureSourceEntry],
) -> bool {
    expected.len() == actual.len()
        && expected.iter().zip(actual).all(|(left, right)| {
            left.relative == right.relative
                && left.kind == right.kind
                && left.len == right.len
                && match left.kind {
                    SecureSourceKind::Directory => left.identity.stable_eq(right.identity),
                    SecureSourceKind::RegularFile => left.identity == right.identity,
                }
        })
}

fn created_manifests_match(expected: &[SecureCreatedEntry], actual: &[SecureCreatedEntry]) -> bool {
    expected.len() == actual.len()
        && expected.iter().all(|left| {
            actual
                .binary_search_by(|right| right.relative.cmp(&left.relative))
                .ok()
                .map(|index| &actual[index])
                .is_some_and(|right| {
                    left.kind == right.kind
                        && match left.kind {
                            SecureSourceKind::Directory => left.identity.stable_eq(right.identity),
                            SecureSourceKind::RegularFile => left.identity == right.identity,
                        }
                })
        })
}

fn created_and_source_manifest_shapes_match(
    created: &[SecureCreatedEntry],
    source: &[SecureSourceEntry],
) -> bool {
    created.len() == source.len().saturating_add(1)
        && created.first().is_some_and(|entry| {
            entry.relative.as_os_str().is_empty() && entry.kind == SecureSourceKind::Directory
        })
        && created[1..].iter().zip(source).all(|(created, source)| {
            created.relative == source.relative && created.kind == source.kind
        })
}

#[cfg(unix)]
fn verify_retained_staging_after_publication(
    staging: &SecureSnapshotStagingDirectory,
    target: &Path,
    operation: &str,
) -> Result<()> {
    let operation_live_retained = staging.operation_live_retained_bytes()?;
    let (observed, observed_bytes) = measure_created_tree(&staging.root, operation_live_retained)?;
    admit_operation_retained_bytes(operation_live_retained, observed_bytes, operation, target)?;
    if !created_manifests_match(&staging.created, &observed) {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} tree identity changed at {}",
            target.display()
        )));
    }
    Ok(())
}

fn measure_tree(
    root: &SecureDirectoryAnchor,
    operation_baseline_bytes: usize,
) -> Result<(RestoreDirectoryMeasurement, Vec<SecureSourceEntry>, usize)> {
    let mut measurement = RestoreDirectoryMeasurement {
        logical_bytes: 0,
        entry_count: 1,
        max_directory_depth: 0,
    };
    let mut entries = Vec::new();
    let mut manifest_memory = ManifestMemoryTracker {
        retained_bytes: 0,
        operation_baseline_bytes,
    };
    let root_directory = SecureOpenedDirectory {
        file: root
            .root
            .try_clone()
            .map_err(|source| TsinkError::IoWithPath {
                path: root.display_path.clone(),
                source,
            })?,
        #[cfg(windows)]
        component_locks: Vec::new(),
    };
    measure_directory(
        root,
        Path::new(""),
        0,
        &root_directory,
        &mut measurement,
        &mut entries,
        &mut manifest_memory,
    )?;
    entries.sort_unstable_by(|left, right| left.relative.cmp(&right.relative));
    Ok((measurement, entries, manifest_memory.retained_bytes))
}

fn measure_directory(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    depth: u32,
    directory: &SecureOpenedDirectory,
    measurement: &mut RestoreDirectoryMeasurement,
    entries: &mut Vec<SecureSourceEntry>,
    manifest_memory: &mut ManifestMemoryTracker,
) -> Result<()> {
    let display = root.display_path.join(relative);
    for_each_directory_name(directory, &display, |name| {
        let child_relative = relative.join(&name);
        let child_display = root.display_path.join(&child_relative);
        let probed_kind = probe_child_entry_kind_nofollow(directory, &name, &child_display)?;
        match probed_kind {
            ProbedEntryKind::Directory | ProbedEntryKind::RegularFile => {}
            ProbedEntryKind::LinkLike => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "unsupported link-like entry while measuring snapshot: {}",
                    child_display.display()
                )));
            }
            ProbedEntryKind::Other => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "unsupported non-file entry while measuring snapshot: {}",
                    child_display.display()
                )));
            }
        }
        let probe = open_child_entry_probe(directory, &name, &child_display)?;
        let metadata = probe_metadata(&probe, &child_display)?;
        if is_link_or_reparse_point(&metadata) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "unsupported link-like entry while measuring snapshot: {}",
                child_display.display()
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
                child_display.display()
            )));
        }
        let identity = identity_from_file(&probe.file, &child_display)?;
        if probed_kind == ProbedEntryKind::Directory && metadata.file_type().is_dir() {
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                TsinkError::Other(
                    "snapshot restore directory depth exceeds the supported range".to_string(),
                )
            })?;
            if child_depth > crate::MAX_SNAPSHOT_RESTORE_DEPTH {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "snapshot restore directory depth {child_depth} exceeds limit {} at {}",
                    crate::MAX_SNAPSHOT_RESTORE_DEPTH,
                    child_display.display()
                )));
            }
            manifest_memory.push_source(
                entries,
                SecureSourceEntry {
                    relative: child_relative.clone(),
                    kind: SecureSourceKind::Directory,
                    len: 0,
                    permissions: metadata.permissions(),
                    identity,
                },
            )?;
            measurement.max_directory_depth = measurement.max_directory_depth.max(child_depth);
            let child_directory =
                open_child_directory(directory, &name, &child_display, &identity)?;
            measure_directory(
                root,
                &child_relative,
                child_depth,
                &child_directory,
                measurement,
                entries,
                manifest_memory,
            )
        } else if probed_kind == ProbedEntryKind::RegularFile && metadata.file_type().is_file() {
            measurement.logical_bytes = measurement
                .logical_bytes
                .checked_add(metadata.len())
                .ok_or_else(|| {
                    TsinkError::Other(format!(
                        "snapshot byte count exceeds the supported range while measuring {}",
                        child_display.display()
                    ))
                })?;
            manifest_memory.push_source(
                entries,
                SecureSourceEntry {
                    relative: child_relative,
                    kind: SecureSourceKind::RegularFile,
                    len: metadata.len(),
                    permissions: metadata.permissions(),
                    identity,
                },
            )?;
            Ok(())
        } else {
            Err(TsinkError::InvalidConfiguration(format!(
                "unsupported non-file entry while measuring snapshot: {}",
                child_display.display()
            )))
        }
    })
}

fn measure_created_tree(
    root: &SecureDirectoryAnchor,
    operation_baseline_bytes: usize,
) -> Result<(Vec<SecureCreatedEntry>, usize)> {
    measure_created_tree_with_limits(
        root,
        operation_baseline_bytes,
        usize::try_from(crate::MAX_SNAPSHOT_RESTORE_ENTRIES).unwrap_or(usize::MAX),
        crate::MAX_SNAPSHOT_RESTORE_DEPTH,
    )
}

fn measure_created_tree_with_limits(
    root: &SecureDirectoryAnchor,
    operation_baseline_bytes: usize,
    max_entries: usize,
    max_depth: u32,
) -> Result<(Vec<SecureCreatedEntry>, usize)> {
    let mut entries = vec![SecureCreatedEntry {
        relative: PathBuf::new(),
        kind: SecureSourceKind::Directory,
        identity: identity_from_file(&root.root, &root.display_path)?,
    }];
    let mut manifest_memory = ManifestMemoryTracker {
        retained_bytes: entries
            .capacity()
            .checked_mul(std::mem::size_of::<SecureCreatedEntry>())
            .ok_or_else(|| {
                TsinkError::Other(
                    "secure snapshot verification manifest accounting overflow".to_string(),
                )
            })?,
        operation_baseline_bytes,
    };
    let root_directory = SecureOpenedDirectory {
        file: root
            .root
            .try_clone()
            .map_err(|source| TsinkError::IoWithPath {
                path: root.display_path.clone(),
                source,
            })?,
        #[cfg(windows)]
        component_locks: Vec::new(),
    };
    {
        let mut state = CreatedTreeMeasurementState {
            entries: &mut entries,
            manifest_memory: &mut manifest_memory,
            max_entries,
            max_depth,
        };
        measure_created_directory(root, Path::new(""), 0, &root_directory, &mut state)?;
    }
    entries.sort_unstable_by(|left, right| left.relative.cmp(&right.relative));
    Ok((entries, manifest_memory.retained_bytes))
}

struct CreatedTreeMeasurementState<'a> {
    entries: &'a mut Vec<SecureCreatedEntry>,
    manifest_memory: &'a mut ManifestMemoryTracker,
    max_entries: usize,
    max_depth: u32,
}

fn measure_created_directory(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    depth: u32,
    directory: &SecureOpenedDirectory,
    state: &mut CreatedTreeMeasurementState<'_>,
) -> Result<()> {
    let display = root.display_path.join(relative);
    for_each_directory_name(directory, &display, |name| {
        if state.entries.len() >= state.max_entries {
            return Err(TsinkError::DataCorruption(format!(
                "snapshot staging namespace exceeds its {}-entry bound: {}",
                state.max_entries,
                root.display_path.display()
            )));
        }
        let child = relative.join(name);
        let child_display = root.display_path.join(&child);
        let probed_kind =
            probe_child_entry_kind_nofollow(directory, child.file_name().unwrap(), &child_display)?;
        match probed_kind {
            ProbedEntryKind::Directory | ProbedEntryKind::RegularFile => {}
            ProbedEntryKind::LinkLike => {
                return Err(TsinkError::DataCorruption(format!(
                    "snapshot staging contains a link-like entry: {}",
                    child_display.display()
                )));
            }
            ProbedEntryKind::Other => {
                return Err(TsinkError::DataCorruption(format!(
                    "snapshot staging contains a non-file entry: {}",
                    child_display.display()
                )));
            }
        }
        let probe = open_child_entry_probe(directory, child.file_name().unwrap(), &child_display)?;
        let metadata = probe_metadata(&probe, &child_display)?;
        let identity = identity_from_file(&probe.file, &child_display)?;
        if is_link_or_reparse_point(&metadata) {
            return Err(TsinkError::DataCorruption(format!(
                "snapshot staging contains a link-like entry: {}",
                child_display.display()
            )));
        }
        let kind = if probed_kind == ProbedEntryKind::Directory && metadata.file_type().is_dir() {
            SecureSourceKind::Directory
        } else if probed_kind == ProbedEntryKind::RegularFile && metadata.file_type().is_file() {
            SecureSourceKind::RegularFile
        } else {
            return Err(TsinkError::DataCorruption(format!(
                "snapshot staging contains a non-file entry: {}",
                child_display.display()
            )));
        };
        state.manifest_memory.push_created(
            state.entries,
            SecureCreatedEntry {
                relative: child.clone(),
                kind,
                identity,
            },
        )?;
        if kind == SecureSourceKind::Directory {
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                TsinkError::DataCorruption("snapshot staging directory depth overflow".to_string())
            })?;
            if child_depth > state.max_depth {
                return Err(TsinkError::DataCorruption(format!(
                    "snapshot staging directory depth {child_depth} exceeds limit {}: {}",
                    state.max_depth,
                    child_display.display()
                )));
            }
            let child_directory = open_child_directory(
                directory,
                child.file_name().unwrap(),
                &child_display,
                &identity,
            )?;
            measure_created_directory(root, &child, child_depth, &child_directory, state)?;
        }
        Ok(())
    })
}

fn validate_opened_source_file(
    file: &File,
    expected_identity: &ClosedSnapshotIdentity,
    expected_len: u64,
    display: &Path,
) -> Result<()> {
    let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: display.to_path_buf(),
        source,
    })?;
    let identity = identity_from_file(file, display)?;
    if is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() != expected_len
        || &identity != expected_identity
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot source changed type, length, or identity while opening: {}",
            display.display()
        )));
    }
    Ok(())
}

fn identity_from_file(file: &File, display: &Path) -> Result<ClosedSnapshotIdentity> {
    ClosedSnapshotIdentity::from_open_file(file, display)
}

fn probe_metadata(probe: &SecureOpenedFile, display: &Path) -> Result<Metadata> {
    probe
        .file
        .metadata()
        .map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })
}

fn validate_single_component(component: &OsStr, display: &Path) -> Result<()> {
    let mut components = Path::new(component).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path component is not a normal single component: {}",
            display.display()
        )));
    }
    Ok(())
}

fn validate_normalized_relative(path: &Path, allow_empty: bool) -> Result<()> {
    if path.as_os_str().is_empty() {
        if allow_empty {
            return Ok(());
        }
        return Err(TsinkError::InvalidConfiguration(
            "secure snapshot relative path must not be empty".to_string(),
        ));
    }
    if path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        Ok(())
    } else {
        Err(TsinkError::InvalidConfiguration(format!(
            "secure snapshot path must be normalized and relative: {}",
            path.display()
        )))
    }
}

#[cfg(unix)]
fn c_component(name: &OsStr, display: &Path) -> Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| {
        TsinkError::InvalidConfiguration(format!(
            "snapshot path contains an interior NUL byte: {}",
            display.display()
        ))
    })
}

#[cfg(unix)]
fn open_linux_directory_at(parent: &File, name: &OsStr, display: &Path) -> Result<File> {
    let name = c_component(name, display)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_existing_directory_chain_nofollow(
    path: &Path,
    operation: &str,
) -> Result<SecureDirectoryAnchor> {
    let requested_metadata =
        std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if is_link_or_reparse_point(&requested_metadata) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} final path must not be a symlink: {}",
            path.display()
        )));
    }
    let resolved = std::fs::canonicalize(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    let (mut current, mut display) = if resolved.is_absolute() {
        (
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(Path::new("/"))
                .map_err(|source| TsinkError::IoWithPath {
                    path: PathBuf::from("/"),
                    source,
                })?,
            PathBuf::from("/"),
        )
    } else {
        (
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(Path::new("."))
                .map_err(TsinkError::Io)?,
            PathBuf::from("."),
        )
    };
    for component in resolved.components() {
        match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => {
                display.push(name);
                current = open_linux_directory_at(&current, name, &display)?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "{operation} path must not contain parent traversal or a platform prefix: {}",
                    path.display()
                )));
            }
        }
    }
    let metadata = current
        .metadata()
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.file_type().is_dir() || is_link_or_reparse_point(&metadata) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} is not a plain directory: {}",
            path.display()
        )));
    }
    #[cfg(test)]
    invoke_anchor_resolution_hook(path);
    // Re-open the requested final component with O_NOFOLLOW after canonical resolution. This
    // closes the metadata/canonicalize race where a final directory is replaced by a symlink whose
    // target happens to match the resolved handle.
    let requested_identity = closed_identity_from_nofollow_directory_path(path, operation)?;
    let opened_identity = identity_from_file(&current, path)?;
    if !opened_identity.stable_eq(requested_identity) {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} changed while its symlink-resolved ancestor chain was opened: {}",
            path.display()
        )));
    }
    Ok(SecureDirectoryAnchor {
        display_path: path.to_path_buf(),
        root: current,
    })
}

#[cfg(unix)]
fn create_directory_chain_nofollow(path: &Path, operation: &str) -> Result<SecureDirectoryAnchor> {
    let resolved = resolve_creatable_unix_path(path, operation)?;
    let (mut current, mut display) = if resolved.is_absolute() {
        (
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(Path::new("/"))
                .map_err(|source| TsinkError::IoWithPath {
                    path: PathBuf::from("/"),
                    source,
                })?,
            PathBuf::from("/"),
        )
    } else {
        (
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(Path::new("."))
                .map_err(TsinkError::Io)?,
            PathBuf::from("."),
        )
    };
    for component in resolved.components() {
        match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => {
                display.push(name);
                match open_linux_directory_at(&current, name, &display) {
                    Ok(next) => current = next,
                    Err(TsinkError::IoWithPath { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        let encoded = c_component(name, &display)?;
                        let created =
                            unsafe { libc::mkdirat(current.as_raw_fd(), encoded.as_ptr(), 0o755) };
                        if created != 0 {
                            return Err(TsinkError::IoWithPath {
                                path: display.clone(),
                                source: std::io::Error::last_os_error(),
                            });
                        }
                        sync_file_as_directory(
                            &current,
                            display.parent().unwrap_or(Path::new(".")),
                        )?;
                        current = open_linux_directory_at(&current, name, &display)?;
                    }
                    Err(err) => return Err(err),
                }
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "{operation} path must not contain parent traversal or a platform prefix: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(SecureDirectoryAnchor {
        display_path: path.to_path_buf(),
        root: current,
    })
}

#[cfg(unix)]
fn resolve_creatable_unix_path(path: &Path, operation: &str) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(TsinkError::Io)?.join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} path must not contain parent traversal: {}",
            path.display()
        )));
    }
    let mut cursor = absolute.as_path();
    let mut missing = Vec::<OsString>::new();
    let resolved_base = loop {
        match std::fs::canonicalize(cursor) {
            Ok(resolved) => break resolved,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let name = cursor.file_name().ok_or_else(|| {
                    TsinkError::InvalidConfiguration(format!(
                        "{operation} cannot resolve a creatable ancestor: {}",
                        path.display()
                    ))
                })?;
                missing.push(name.to_os_string());
                cursor = cursor.parent().ok_or_else(|| {
                    TsinkError::InvalidConfiguration(format!(
                        "{operation} has no existing ancestor: {}",
                        path.display()
                    ))
                })?;
            }
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: cursor.to_path_buf(),
                    source,
                });
            }
        }
    };
    let mut resolved = resolved_base;
    for component in missing.into_iter().rev() {
        validate_single_component(&component, path)?;
        resolved.push(component);
    }
    Ok(resolved)
}

#[cfg(unix)]
fn closed_identity_from_nofollow_directory_path(
    path: &Path,
    operation: &str,
) -> Result<ClosedSnapshotIdentity> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} is not a directory: {}",
            path.display()
        )));
    }
    identity_from_file(&file, path)
}

#[cfg(unix)]
fn open_relative_directory(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedDirectory> {
    validate_normalized_relative(relative, true)?;
    let mut current = root
        .root
        .try_clone()
        .map_err(|source| TsinkError::IoWithPath {
            path: root.display_path.clone(),
            source,
        })?;
    let mut display = root.display_path.clone();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            unreachable!("relative path validated");
        };
        display.push(name);
        current = open_linux_directory_at(&current, name, &display)?;
    }
    let identity = identity_from_file(&current, &display)?;
    if !identity.stable_eq(*expected) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot directory identity changed while opening: {}",
            display.display()
        )));
    }
    Ok(SecureOpenedDirectory { file: current })
}

#[cfg(unix)]
fn open_child_directory(
    parent: &SecureOpenedDirectory,
    name: &OsStr,
    display: &Path,
    expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedDirectory> {
    let file = open_linux_directory_at(&parent.file, name, display)?;
    let identity = identity_from_file(&file, display)?;
    if !identity.stable_eq(*expected) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot child directory identity changed while opening: {}",
            display.display()
        )));
    }
    Ok(SecureOpenedDirectory { file })
}

#[cfg(unix)]
fn open_child_entry_probe(
    parent: &SecureOpenedDirectory,
    name: &OsStr,
    display: &Path,
) -> Result<SecureOpenedFile> {
    let encoded = c_component(name, display)?;
    let fd = unsafe {
        libc::openat(
            parent.file.as_raw_fd(),
            encoded.as_ptr(),
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(SecureOpenedFile {
        file: unsafe { File::from_raw_fd(fd) },
    })
}

#[cfg(unix)]
fn probe_child_entry_kind_nofollow(
    parent: &SecureOpenedDirectory,
    name: &OsStr,
    display: &Path,
) -> Result<ProbedEntryKind> {
    let encoded = c_component(name, display)?;
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    let inspected = unsafe {
        libc::fstatat(
            parent.file.as_raw_fd(),
            encoded.as_ptr(),
            status.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if inspected != 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let status = unsafe { status.assume_init() };
    let file_type = status.st_mode & libc::S_IFMT;
    Ok(if file_type == libc::S_IFDIR {
        ProbedEntryKind::Directory
    } else if file_type == libc::S_IFREG {
        ProbedEntryKind::RegularFile
    } else if file_type == libc::S_IFLNK {
        ProbedEntryKind::LinkLike
    } else {
        ProbedEntryKind::Other
    })
}

#[cfg(unix)]
fn probe_relative_entry_kind_nofollow(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<ProbedEntryKind> {
    let (parent, name, display) = open_relative_parent(root, relative, expected_parent)?;
    probe_child_entry_kind_nofollow(&parent, name, &display)
}

#[cfg(unix)]
fn open_relative_entry_probe(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedFile> {
    let (parent, name, display) = open_relative_parent(root, relative, expected_parent)?;
    let encoded = c_component(name, &display)?;
    let fd = unsafe {
        libc::openat(
            parent.file.as_raw_fd(),
            encoded.as_ptr(),
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display,
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(SecureOpenedFile {
        file: unsafe { File::from_raw_fd(fd) },
    })
}

#[cfg(unix)]
fn open_relative_regular_file(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected_parent: &ClosedSnapshotIdentity,
    expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedFile> {
    let (parent, name, display) = open_relative_parent(root, relative, expected_parent)?;
    let encoded = c_component(name, &display)?;
    let fd = unsafe {
        libc::openat(
            parent.file.as_raw_fd(),
            encoded.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.clone(),
            source: std::io::Error::last_os_error(),
        });
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let identity = identity_from_file(&file, &display)?;
    if &identity != expected {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot file identity changed while opening: {}",
            display.display()
        )));
    }
    Ok(SecureOpenedFile { file })
}

#[cfg(unix)]
fn open_relative_parent<'a>(
    root: &SecureDirectoryAnchor,
    relative: &'a Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedDirectory, &'a OsStr, PathBuf)> {
    validate_normalized_relative(relative, false)?;
    let name = relative
        .file_name()
        .expect("validated nonempty relative path");
    let parent_relative = relative.parent().unwrap_or(Path::new(""));
    let parent = if parent_relative.as_os_str().is_empty() {
        let opened = SecureOpenedDirectory {
            file: root
                .root
                .try_clone()
                .map_err(|source| TsinkError::IoWithPath {
                    path: root.display_path.clone(),
                    source,
                })?,
        };
        let actual = identity_from_file(&opened.file, &root.display_path)?;
        if !actual.stable_eq(*expected_parent) {
            return Err(TsinkError::DataCorruption(format!(
                "secure snapshot root identity changed while opening a child: {}",
                root.display_path.display()
            )));
        }
        opened
    } else {
        open_relative_directory(root, parent_relative, expected_parent)?
    };
    Ok((parent, name, root.display_path.join(relative)))
}

#[cfg(unix)]
fn create_staging_at_parent(
    parent: &SecureDirectoryAnchor,
    name: &OsStr,
    display: &Path,
) -> Result<SecureDirectoryAnchor> {
    let encoded = c_component(name, display)?;
    let created = unsafe { libc::mkdirat(parent.root.as_raw_fd(), encoded.as_ptr(), 0o700) };
    if created != 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let root = open_linux_directory_at(&parent.root, name, display)?;
    Ok(SecureDirectoryAnchor {
        display_path: display.to_path_buf(),
        root,
    })
}

#[cfg(unix)]
fn publish_staging_noreplace(
    staging: SecureSnapshotStagingDirectory,
    requested_target: &Path,
    target: &Path,
    target_name: &OsStr,
) -> std::result::Result<(), SecureSnapshotPublicationError> {
    let source = c_component(&staging.root_name, &staging.display_path)
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    let destination = c_component(target_name, target)
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let renamed = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            staging.parent.root.as_raw_fd(),
            source.as_ptr(),
            staging.parent.root.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        ) as libc::c_int
    };
    #[cfg(target_vendor = "apple")]
    let renamed = unsafe {
        libc::renameatx_np(
            staging.parent.root.as_raw_fd(),
            source.as_ptr(),
            staging.parent.root.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(all(
        unix,
        not(any(target_os = "linux", target_os = "android")),
        not(target_vendor = "apple")
    ))]
    let renamed = {
        return Err(SecureSnapshotPublicationError::before_publication(
            TsinkError::InvalidConfiguration(
                "secure handle-relative no-replace snapshot publication is unsupported on this Unix platform"
                    .to_string(),
            ),
        ));
    };
    if renamed != 0 {
        return Err(SecureSnapshotPublicationError::before_publication(
            TsinkError::IoWithPath {
                path: target.to_path_buf(),
                source: std::io::Error::last_os_error(),
            },
        ));
    }
    #[cfg(test)]
    invoke_publication_after_rename_hook(target);
    let parent_identity = identity_from_file(&staging.parent.root, &staging.parent.display_path)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    attest_relative_identity(
        &staging.parent,
        Path::new(target_name),
        target,
        SecureSourceKind::Directory,
        &parent_identity,
        &staging.created[0].identity,
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    verify_retained_staging_after_publication(
        &staging,
        target,
        "Unix secure snapshot post-publication verification",
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    sync_file_as_directory(&staging.parent.root, &staging.parent.display_path)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    attest_requested_publication_parent(
        &staging.parent,
        requested_target,
        "snapshot post-publication parent attestation",
    )
    .map_err(SecureSnapshotPublicationError::after_publication)
}

#[cfg(unix)]
fn rename_unix_sibling_noreplace(
    parent: &SecureDirectoryAnchor,
    source_name: &OsStr,
    destination_name: &OsStr,
    destination_display: &Path,
) -> Result<()> {
    let source = c_component(source_name, &parent.display_path.join(source_name))?;
    let destination = c_component(destination_name, destination_display)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let renamed = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.root.as_raw_fd(),
            source.as_ptr(),
            parent.root.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        ) as libc::c_int
    };
    #[cfg(target_vendor = "apple")]
    let renamed = unsafe {
        libc::renameatx_np(
            parent.root.as_raw_fd(),
            source.as_ptr(),
            parent.root.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(all(
        unix,
        not(any(target_os = "linux", target_os = "android")),
        not(target_vendor = "apple")
    ))]
    let renamed = {
        return Err(TsinkError::InvalidConfiguration(
            "secure handle-relative no-replace restore activation is unsupported on this Unix platform"
                .to_string(),
        ));
    };
    if renamed != 0 {
        return Err(TsinkError::IoWithPath {
            path: destination_display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn opened_sibling_directory_identity(
    parent: &SecureDirectoryAnchor,
    name: &OsStr,
    display: &Path,
) -> Result<ClosedSnapshotIdentity> {
    let parent_identity = identity_from_file(&parent.root, &parent.display_path)?;
    let probe = open_relative_entry_probe(parent, Path::new(name), &parent_identity)?;
    let metadata = probe_metadata(&probe, display)?;
    let identity = identity_from_file(&probe.file, display)?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "restore replacement source is not a plain directory: {}",
            display.display()
        )));
    }
    Ok(identity)
}

#[cfg(unix)]
fn rollback_unix_backup(
    parent: &SecureDirectoryAnchor,
    target: &Path,
    target_name: &OsStr,
    backup: &Path,
    backup_name: &OsStr,
    expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    let current = opened_sibling_directory_identity(parent, backup_name, backup)?;
    if !current.stable_eq(*expected) {
        return Err(TsinkError::DataCorruption(format!(
            "restore backup identity changed before rollback: {}",
            backup.display()
        )));
    }
    rename_unix_sibling_noreplace(parent, backup_name, target_name, target)?;
    let restored = opened_sibling_directory_identity(parent, target_name, target)?;
    if !restored.stable_eq(*expected) {
        return Err(TsinkError::DataCorruption(format!(
            "restore rollback installed an unexpected target identity: {}",
            target.display()
        )));
    }
    sync_file_as_directory(&parent.root, &parent.display_path)
}

#[cfg(unix)]
fn unix_prepublication_failure_with_rollback(
    primary: TsinkError,
    parent: &SecureDirectoryAnchor,
    target: &Path,
    target_name: &OsStr,
    backup: &Path,
    backup_name: &OsStr,
    expected_backup: &ClosedSnapshotIdentity,
) -> SecureSnapshotPublicationError {
    let error = match rollback_unix_backup(
        parent,
        target,
        target_name,
        backup,
        backup_name,
        expected_backup,
    ) {
        Ok(()) => TsinkError::Other(format!(
            "restore activation failed before staging publication: {primary}; original target was restored from the retained backup"
        )),
        Err(rollback) => TsinkError::Other(format!(
            "restore activation failed before staging publication: {primary}; identity-attested backup rollback failed: {rollback}; staging and any surviving backup are retained"
        )),
    };
    SecureSnapshotPublicationError::before_publication(error)
}

#[cfg(unix)]
fn publish_staging_replacing(
    staging: SecureSnapshotStagingDirectory,
    paths: SecureSnapshotReplacementPaths<'_>,
    original_target: SecureSnapshotSourceTree,
    external_operation_baseline_bytes: usize,
) -> std::result::Result<SecureSnapshotPublishedBackup, SecureSnapshotPublicationError> {
    let SecureSnapshotReplacementPaths {
        requested_target,
        target,
        target_name,
        requested_backup,
        backup,
        backup_name,
    } = paths;
    if relative_entry_exists_nofollow(&staging.parent, Path::new(backup_name))
        .map_err(SecureSnapshotPublicationError::before_publication)?
    {
        return Err(SecureSnapshotPublicationError::before_publication(
            TsinkError::InvalidConfiguration(format!(
                "restore backup destination already exists: {}",
                backup.display()
            )),
        ));
    }
    let expected_backup = opened_sibling_directory_identity(&staging.parent, target_name, target)
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    if !expected_backup.stable_eq(original_target.root_identity) {
        return Err(SecureSnapshotPublicationError::before_publication(
            TsinkError::DataCorruption(format!(
                "restore replacement target does not match its pre-move identity manifest: {}",
                target.display()
            )),
        ));
    }
    rename_unix_sibling_noreplace(&staging.parent, target_name, backup_name, backup)
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    let moved_backup = opened_sibling_directory_identity(&staging.parent, backup_name, backup)
        .map_err(|error| {
            unix_prepublication_failure_with_rollback(
                error,
                &staging.parent,
                target,
                target_name,
                backup,
                backup_name,
                &expected_backup,
            )
        })?;
    if !moved_backup.stable_eq(expected_backup) {
        return Err(unix_prepublication_failure_with_rollback(
            TsinkError::DataCorruption(format!(
                "restore target-to-backup move changed directory identity: {}",
                backup.display()
            )),
            &staging.parent,
            target,
            target_name,
            backup,
            backup_name,
            &expected_backup,
        ));
    }
    if let Err(error) = sync_file_as_directory(&staging.parent.root, &staging.parent.display_path) {
        return Err(unix_prepublication_failure_with_rollback(
            error,
            &staging.parent,
            target,
            target_name,
            backup,
            backup_name,
            &expected_backup,
        ));
    }
    if let Err(error) =
        rename_unix_sibling_noreplace(&staging.parent, &staging.root_name, target_name, target)
    {
        return Err(unix_prepublication_failure_with_rollback(
            error,
            &staging.parent,
            target,
            target_name,
            backup,
            backup_name,
            &expected_backup,
        ));
    }

    #[cfg(test)]
    invoke_publication_after_rename_hook(target);
    let parent_identity = identity_from_file(&staging.parent.root, &staging.parent.display_path)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    attest_relative_identity(
        &staging.parent,
        Path::new(target_name),
        target,
        SecureSourceKind::Directory,
        &parent_identity,
        &staging.created[0].identity,
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    verify_retained_staging_after_publication(
        &staging,
        target,
        "Unix secure restore post-publication verification",
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    sync_file_as_directory(&staging.parent.root, &staging.parent.display_path)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    attest_requested_publication_parent(
        &staging.parent,
        requested_target,
        "restore post-publication target-parent attestation",
    )
    .and_then(|()| {
        attest_requested_publication_parent(
            &staging.parent,
            requested_backup,
            "restore post-publication backup-parent attestation",
        )
    })
    .map_err(SecureSnapshotPublicationError::after_publication)?;

    let SecureSnapshotSourceTree {
        mut root,
        root_identity,
        measurement,
        entries,
        ..
    } = original_target;
    root.display_path = backup.to_path_buf();
    let mut published_backup = SecureSnapshotPublishedBackup {
        parent: staging.parent,
        root,
        requested_backup_path: requested_backup.to_path_buf(),
        backup_path: backup.to_path_buf(),
        backup_name: backup_name.to_os_string(),
        root_identity,
        measurement,
        entries,
        retained_memory_bytes: 0,
        external_operation_baseline_bytes,
    };
    published_backup.retained_memory_bytes = published_backup_retained_bytes(&published_backup)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    admit_operation_retained_bytes(
        published_backup.external_operation_baseline_bytes,
        published_backup.retained_memory_bytes,
        "secure restore published-backup cleanup token",
        backup,
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    Ok(published_backup)
}

#[cfg(unix)]
fn remove_published_backup_exact(backup: SecureSnapshotPublishedBackup) -> Result<()> {
    for entry in backup.entries.iter().rev() {
        let expected_parent = *backup.parent_identity(&entry.relative)?;
        let (parent, name, display) =
            open_relative_parent(&backup.root, &entry.relative, &expected_parent)?;
        let probe = open_child_entry_probe(&parent, name, &display)?;
        let metadata = probe_metadata(&probe, &display)?;
        let actual = identity_from_file(&probe.file, &display)?;
        let matches = !is_link_or_reparse_point(&metadata)
            && match entry.kind {
                SecureSourceKind::Directory => {
                    metadata.file_type().is_dir() && actual.stable_eq(entry.identity)
                }
                SecureSourceKind::RegularFile => {
                    metadata.file_type().is_file()
                        && metadata.len() == entry.len
                        && actual == entry.identity
                }
            };
        if !matches {
            return Err(TsinkError::DataCorruption(format!(
                "restore backup entry changed before exact cleanup: {}",
                display.display()
            )));
        }
        drop(probe);
        let encoded = c_component(name, &display)?;
        let flags = if entry.kind == SecureSourceKind::Directory {
            libc::AT_REMOVEDIR
        } else {
            0
        };
        let removed = unsafe { libc::unlinkat(parent.file.as_raw_fd(), encoded.as_ptr(), flags) };
        if removed != 0 {
            return Err(TsinkError::IoWithPath {
                path: display,
                source: std::io::Error::last_os_error(),
            });
        }
        sync_opened_directory(
            &parent,
            backup
                .backup_path
                .join(&entry.relative)
                .parent()
                .unwrap_or(&backup.backup_path),
        )?;
    }

    let current_root = identity_from_file(&backup.root.root, &backup.backup_path)?;
    if !current_root.stable_eq(backup.root_identity) {
        return Err(TsinkError::DataCorruption(format!(
            "restore backup root changed before exact cleanup: {}",
            backup.backup_path.display()
        )));
    }
    attest_root_entry(
        &backup.parent,
        &backup.backup_name,
        &backup.backup_path,
        &backup.root_identity,
    )?;
    let encoded = c_component(&backup.backup_name, &backup.backup_path)?;
    let removed = unsafe {
        libc::unlinkat(
            backup.parent.root.as_raw_fd(),
            encoded.as_ptr(),
            libc::AT_REMOVEDIR,
        )
    };
    if removed != 0 {
        return Err(TsinkError::IoWithPath {
            path: backup.backup_path.clone(),
            source: std::io::Error::last_os_error(),
        });
    }
    sync_file_as_directory(&backup.parent.root, &backup.parent.display_path)?;
    attest_requested_publication_parent(
        &backup.parent,
        &backup.requested_backup_path,
        "restore post-cleanup backup-parent attestation",
    )
}

#[cfg(unix)]
fn create_relative_directory(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    display: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedDirectory, SecureOpenedDirectory)> {
    let (parent, name, _) = open_relative_parent(root, relative, expected_parent)?;
    let encoded = c_component(name, display)?;
    let created = unsafe { libc::mkdirat(parent.file.as_raw_fd(), encoded.as_ptr(), 0o700) };
    if created != 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let directory = open_linux_directory_at(&parent.file, name, display)?;
    Ok((SecureOpenedDirectory { file: directory }, parent))
}

#[cfg(unix)]
fn create_relative_regular_file(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    display: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedFile, SecureOpenedDirectory)> {
    let (parent, name, _) = open_relative_parent(root, relative, expected_parent)?;
    let encoded = c_component(name, display)?;
    let fd = unsafe {
        libc::openat(
            parent.file.as_raw_fd(),
            encoded.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok((
        SecureOpenedFile {
            file: unsafe { File::from_raw_fd(fd) },
        },
        parent,
    ))
}

#[cfg(unix)]
fn relative_entry_exists_nofollow(parent: &SecureDirectoryAnchor, relative: &Path) -> Result<bool> {
    let parent_identity = identity_from_file(&parent.root, &parent.display_path)?;
    let probe = open_relative_entry_probe(parent, relative, &parent_identity);
    match probe {
        Ok(_) => Ok(true),
        Err(TsinkError::IoWithPath { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn attest_relative_identity(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    display: &Path,
    kind: SecureSourceKind,
    expected_parent: &ClosedSnapshotIdentity,
    expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    let probe = open_relative_entry_probe(root, relative, expected_parent)?;
    let current = identity_from_file(&probe.file, display)?;
    let matches = match kind {
        SecureSourceKind::Directory => current.stable_eq(*expected),
        SecureSourceKind::RegularFile => &current == expected,
    };
    if !matches {
        return Err(TsinkError::DataCorruption(format!(
            "created staging entry identity changed before attestation: {}",
            display.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn attest_root_entry(
    parent: &SecureDirectoryAnchor,
    root_name: &OsStr,
    display: &Path,
    expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    attest_relative_identity(
        parent,
        Path::new(root_name),
        display,
        SecureSourceKind::Directory,
        &identity_from_file(&parent.root, &parent.display_path)?,
        expected,
    )
}

#[cfg(unix)]
fn for_each_directory_name(
    directory: &SecureOpenedDirectory,
    display: &Path,
    mut visit: impl FnMut(OsString) -> Result<()>,
) -> Result<()> {
    // Opening "." relative to the retained directory creates an independent open-file
    // description. A plain dup would share the directory offset with the anchor and make a later
    // verification scan start at EOF.
    let duplicated = unsafe {
        libc::openat(
            directory.file.as_raw_fd(),
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if duplicated < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let stream = unsafe { libc::fdopendir(duplicated) };
    if stream.is_null() {
        let source = std::io::Error::last_os_error();
        unsafe {
            libc::close(duplicated);
        }
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        });
    }
    struct DirectoryStream(*mut libc::DIR);
    impl Drop for DirectoryStream {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let stream = DirectoryStream(stream);
    unsafe {
        libc::rewinddir(stream.0);
    }
    loop {
        clear_readdir_errno();
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let errno = readdir_errno();
            if errno != 0 {
                return Err(TsinkError::IoWithPath {
                    path: display.to_path_buf(),
                    source: std::io::Error::from_raw_os_error(errno),
                });
            }
            break;
        }
        let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }
        visit(OsString::from_vec(bytes.to_vec()))?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn clear_readdir_errno() {
    unsafe {
        *libc::__errno_location() = 0;
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn readdir_errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

#[cfg(target_vendor = "apple")]
fn clear_readdir_errno() {
    unsafe {
        *libc::__error() = 0;
    }
}

#[cfg(target_vendor = "apple")]
fn readdir_errno() -> libc::c_int {
    unsafe { *libc::__error() }
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android")),
    not(target_vendor = "apple")
))]
fn clear_readdir_errno() {}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android")),
    not(target_vendor = "apple")
))]
fn readdir_errno() -> libc::c_int {
    0
}

#[cfg(unix)]
fn sync_opened_directory(directory: &SecureOpenedDirectory, display: &Path) -> Result<()> {
    sync_file_as_directory(&directory.file, display)
}

#[cfg(unix)]
fn sync_file_as_directory(file: &File, display: &Path) -> Result<()> {
    #[cfg(test)]
    invoke_directory_sync_hook(display)?;
    file.sync_all().map_err(|source| TsinkError::IoWithPath {
        path: display.to_path_buf(),
        source,
    })
}

#[cfg(windows)]
const WINDOWS_FILE_SHARE_READ: u32 = 0x0000_0001;
#[cfg(windows)]
const WINDOWS_FILE_SHARE_WRITE: u32 = 0x0000_0002;
#[cfg(windows)]
const WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
#[cfg(windows)]
const WINDOWS_FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
#[cfg(windows)]
const WINDOWS_FILE_READ_ATTRIBUTES: u32 = 0x0000_0080;

#[cfg(windows)]
fn open_windows_directory(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .share_mode(WINDOWS_FILE_SHARE_READ | WINDOWS_FILE_SHARE_WRITE)
        .custom_flags(WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT | WINDOWS_FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot directory component is link-like or not a directory: {}",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(windows)]
fn windows_absolute_no_parent(path: &Path, operation: &str) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(TsinkError::Io)?.join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} path must not contain parent traversal: {}",
            path.display()
        )));
    }
    Ok(absolute)
}

#[cfg(windows)]
fn windows_root_and_names(path: &Path, operation: &str) -> Result<(PathBuf, Vec<OsString>)> {
    let absolute = windows_absolute_no_parent(path, operation)?;
    let mut root = PathBuf::new();
    let mut names = Vec::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => root.push(prefix.as_os_str()),
            Component::RootDir => root.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(name) => names.push(name.to_os_string()),
            Component::ParentDir => unreachable!("parent traversal rejected"),
        }
    }
    if root.as_os_str().is_empty() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} path has no Windows volume root: {}",
            path.display()
        )));
    }
    Ok((root, names))
}

#[cfg(windows)]
fn open_existing_directory_chain_nofollow(
    path: &Path,
    operation: &str,
) -> Result<SecureDirectoryAnchor> {
    let (mut display, names) = windows_root_and_names(path, operation)?;
    let mut current = open_windows_directory(&display)?;
    let mut locks = Vec::new();
    for name in names {
        display.push(&name);
        let next = open_windows_directory(&display)?;
        locks.push(current);
        current = next;
    }
    Ok(SecureDirectoryAnchor {
        display_path: windows_absolute_no_parent(path, operation)?,
        root: current,
        ancestor_locks: locks,
    })
}

#[cfg(windows)]
fn create_directory_chain_nofollow(path: &Path, operation: &str) -> Result<SecureDirectoryAnchor> {
    let absolute = windows_absolute_no_parent(path, operation)?;
    let (mut display, names) = windows_root_and_names(&absolute, operation)?;
    let mut current = open_windows_directory(&display)?;
    let mut locks = Vec::new();
    for name in names {
        display.push(&name);
        let next = match open_windows_directory(&display) {
            Ok(next) => next,
            Err(TsinkError::IoWithPath { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                std::fs::create_dir(&display).map_err(|source| TsinkError::IoWithPath {
                    path: display.clone(),
                    source,
                })?;
                sync_file_as_directory(&current, display.parent().unwrap_or(&display))?;
                open_windows_directory(&display)?
            }
            Err(err) => return Err(err),
        };
        locks.push(current);
        current = next;
    }
    Ok(SecureDirectoryAnchor {
        display_path: absolute,
        root: current,
        ancestor_locks: locks,
    })
}

#[cfg(windows)]
fn open_windows_relative_directory_unchecked(
    root: &SecureDirectoryAnchor,
    relative: &Path,
) -> Result<SecureOpenedDirectory> {
    let mut current = root
        .root
        .try_clone()
        .map_err(|source| TsinkError::IoWithPath {
            path: root.display_path.clone(),
            source,
        })?;
    let mut locks = Vec::new();
    let mut display = root.display_path.clone();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            unreachable!("relative path validated");
        };
        display.push(name);
        let next = open_windows_directory(&display)?;
        locks.push(current);
        current = next;
    }
    Ok(SecureOpenedDirectory {
        file: current,
        component_locks: locks,
    })
}

#[cfg(windows)]
fn open_relative_directory(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedDirectory> {
    validate_normalized_relative(relative, true)?;
    let directory = open_windows_relative_directory_unchecked(root, relative)?;
    let identity = identity_from_file(&directory.file, &root.display_path.join(relative))?;
    if !identity.stable_eq(*expected) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot directory identity changed while opening: {}",
            root.display_path.join(relative).display()
        )));
    }
    Ok(directory)
}

#[cfg(windows)]
fn open_child_directory(
    _parent: &SecureOpenedDirectory,
    _name: &OsStr,
    display: &Path,
    expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedDirectory> {
    let file = open_windows_directory(display)?;
    let identity = identity_from_file(&file, display)?;
    if !identity.stable_eq(*expected) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot child directory identity changed while opening: {}",
            display.display()
        )));
    }
    Ok(SecureOpenedDirectory {
        file,
        component_locks: Vec::new(),
    })
}

#[cfg(windows)]
fn open_child_entry_probe(
    _parent: &SecureOpenedDirectory,
    _name: &OsStr,
    display: &Path,
) -> Result<SecureOpenedFile> {
    let file = OpenOptions::new()
        .access_mode(WINDOWS_FILE_READ_ATTRIBUTES)
        .share_mode(WINDOWS_FILE_SHARE_READ | WINDOWS_FILE_SHARE_WRITE)
        .custom_flags(WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT | WINDOWS_FILE_FLAG_BACKUP_SEMANTICS)
        .open(display)
        .map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })?;
    Ok(SecureOpenedFile {
        file,
        component_locks: Vec::new(),
    })
}

#[cfg(windows)]
fn probed_kind_from_metadata(metadata: &Metadata) -> ProbedEntryKind {
    if is_link_or_reparse_point(metadata) {
        ProbedEntryKind::LinkLike
    } else if metadata.file_type().is_dir() {
        ProbedEntryKind::Directory
    } else if metadata.file_type().is_file() {
        ProbedEntryKind::RegularFile
    } else {
        ProbedEntryKind::Other
    }
}

#[cfg(windows)]
fn probe_child_entry_kind_nofollow(
    _parent: &SecureOpenedDirectory,
    _name: &OsStr,
    display: &Path,
) -> Result<ProbedEntryKind> {
    std::fs::symlink_metadata(display)
        .map(|metadata| probed_kind_from_metadata(&metadata))
        .map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })
}

#[cfg(windows)]
fn probe_relative_entry_kind_nofollow(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<ProbedEntryKind> {
    let (_parent, _name, display) = open_relative_parent(root, relative, expected_parent)?;
    std::fs::symlink_metadata(&display)
        .map(|metadata| probed_kind_from_metadata(&metadata))
        .map_err(|source| TsinkError::IoWithPath {
            path: display,
            source,
        })
}

#[cfg(windows)]
fn open_relative_entry_probe(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedFile> {
    let (parent, _name, display) = open_relative_parent(root, relative, expected_parent)?;
    let file = OpenOptions::new()
        .access_mode(WINDOWS_FILE_READ_ATTRIBUTES)
        .share_mode(WINDOWS_FILE_SHARE_READ | WINDOWS_FILE_SHARE_WRITE)
        .custom_flags(WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT | WINDOWS_FILE_FLAG_BACKUP_SEMANTICS)
        .open(&display)
        .map_err(|source| TsinkError::IoWithPath {
            path: display,
            source,
        })?;
    Ok(SecureOpenedFile {
        file,
        component_locks: parent.component_locks,
    })
}

#[cfg(windows)]
fn open_relative_regular_file(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    expected_parent: &ClosedSnapshotIdentity,
    expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedFile> {
    let (parent, _name, display) = open_relative_parent(root, relative, expected_parent)?;
    let file = OpenOptions::new()
        .read(true)
        // The engine can retain a writer handle to WAL files while the snapshot fence prevents
        // cooperating writes. Allow that handle, but deny delete sharing so this identity cannot
        // be renamed out from under the copy.
        .share_mode(WINDOWS_FILE_SHARE_READ | WINDOWS_FILE_SHARE_WRITE)
        .custom_flags(WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT)
        .open(&display)
        .map_err(|source| TsinkError::IoWithPath {
            path: display.clone(),
            source,
        })?;
    let identity = identity_from_file(&file, &display)?;
    if &identity != expected {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot file identity changed while opening: {}",
            display.display()
        )));
    }
    Ok(SecureOpenedFile {
        file,
        component_locks: parent.component_locks,
    })
}

#[cfg(windows)]
fn open_relative_parent<'a>(
    root: &SecureDirectoryAnchor,
    relative: &'a Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedDirectory, &'a OsStr, PathBuf)> {
    validate_normalized_relative(relative, false)?;
    let name = relative
        .file_name()
        .expect("validated nonempty relative path");
    let parent_relative = relative.parent().unwrap_or(Path::new(""));
    let parent = open_windows_relative_directory_unchecked(root, parent_relative)?;
    let actual = identity_from_file(&parent.file, &root.display_path.join(parent_relative))?;
    if !actual.stable_eq(*expected_parent) {
        return Err(TsinkError::DataCorruption(format!(
            "secure snapshot parent identity changed while opening a child: {}",
            root.display_path.join(parent_relative).display()
        )));
    }
    Ok((parent, name, root.display_path.join(relative)))
}

#[cfg(windows)]
fn create_staging_at_parent(
    parent: &SecureDirectoryAnchor,
    name: &OsStr,
    _display: &Path,
) -> Result<SecureDirectoryAnchor> {
    let anchored_display = parent.display_path.join(name);
    std::fs::create_dir(&anchored_display).map_err(|source| TsinkError::IoWithPath {
        path: anchored_display.clone(),
        source,
    })?;
    let root = open_windows_directory(&anchored_display)?;
    Ok(SecureDirectoryAnchor {
        display_path: anchored_display,
        root,
        ancestor_locks: vec![parent
            .root
            .try_clone()
            .map_err(|source| TsinkError::IoWithPath {
                path: parent.display_path.clone(),
                source,
            })?],
    })
}

#[cfg(windows)]
fn publish_staging_noreplace(
    staging: SecureSnapshotStagingDirectory,
    requested_target: &Path,
    target: &Path,
    _target_name: &OsStr,
) -> std::result::Result<(), SecureSnapshotPublicationError> {
    let operation_live_retained = staging
        .operation_live_retained_bytes()
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    let expected_root = staging.created[0].identity;
    let expected_created = staging.created;
    let source_path = staging.display_path;
    let parent = staging.parent;
    // Windows directory handles opened without FILE_SHARE_DELETE prevent replacement during
    // copy. MoveFileExW cannot rename while that staging-root handle is live, so release only the
    // root handle; the parent and every external ancestor remain locked against rename/delete.
    drop(staging.root);
    super::rename_path_noreplace(&source_path, target)
        .map_err(SecureSnapshotPublicationError::before_publication)?;

    let target_file = open_windows_directory(target)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    let actual_root = identity_from_file(&target_file, target)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    if !actual_root.stable_eq(expected_root) {
        return Err(SecureSnapshotPublicationError::after_publication(
            TsinkError::DataCorruption(format!(
                "Windows snapshot publication moved an unexpected staging identity to {}",
                target.display()
            )),
        ));
    }
    let published = SecureDirectoryAnchor {
        display_path: target.to_path_buf(),
        root: target_file,
        ancestor_locks: Vec::new(),
    };
    let (observed, observed_bytes) = measure_created_tree(&published, operation_live_retained)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    admit_operation_retained_bytes(
        operation_live_retained,
        observed_bytes,
        "Windows secure snapshot post-publication verification",
        target,
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    if !created_manifests_match(&expected_created, &observed) {
        return Err(SecureSnapshotPublicationError::after_publication(
            TsinkError::DataCorruption(format!(
                "Windows snapshot publication tree identity changed at {}",
                target.display()
            )),
        ));
    }
    sync_file_as_directory(&parent.root, &parent.display_path)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    attest_requested_publication_parent(
        &parent,
        requested_target,
        "snapshot post-publication parent attestation",
    )
    .map_err(SecureSnapshotPublicationError::after_publication)
}

#[cfg(windows)]
fn opened_windows_sibling_directory(
    parent: &SecureDirectoryAnchor,
    name: &OsStr,
    display: &Path,
) -> Result<(File, ClosedSnapshotIdentity)> {
    let parent_identity = identity_from_file(&parent.root, &parent.display_path)?;
    let probe = open_relative_entry_probe(parent, Path::new(name), &parent_identity)?;
    let metadata = probe_metadata(&probe, display)?;
    let identity = identity_from_file(&probe.file, display)?;
    if is_link_or_reparse_point(&metadata) || !metadata.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "restore replacement source is not a plain directory: {}",
            display.display()
        )));
    }
    Ok((probe.file, identity))
}

#[cfg(windows)]
fn rollback_windows_backup(
    parent: &SecureDirectoryAnchor,
    target: &Path,
    target_name: &OsStr,
    backup: &Path,
    backup_name: &OsStr,
    expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    let (backup_handle, actual) = opened_windows_sibling_directory(parent, backup_name, backup)?;
    if !actual.stable_eq(*expected) {
        return Err(TsinkError::DataCorruption(format!(
            "restore backup identity changed before rollback: {}",
            backup.display()
        )));
    }
    drop(backup_handle);
    super::rename_path_noreplace(backup, target)?;
    let (_target_handle, restored) = opened_windows_sibling_directory(parent, target_name, target)?;
    if !restored.stable_eq(*expected) {
        return Err(TsinkError::DataCorruption(format!(
            "restore rollback installed an unexpected target identity: {}",
            target.display()
        )));
    }
    sync_file_as_directory(&parent.root, &parent.display_path)
}

#[cfg(windows)]
fn windows_prepublication_failure_with_rollback(
    primary: TsinkError,
    parent: &SecureDirectoryAnchor,
    target: &Path,
    target_name: &OsStr,
    backup: &Path,
    backup_name: &OsStr,
    expected_backup: &ClosedSnapshotIdentity,
) -> SecureSnapshotPublicationError {
    let error = match rollback_windows_backup(
        parent,
        target,
        target_name,
        backup,
        backup_name,
        expected_backup,
    ) {
        Ok(()) => TsinkError::Other(format!(
            "restore activation failed before staging publication: {primary}; original target was restored from the retained backup"
        )),
        Err(rollback) => TsinkError::Other(format!(
            "restore activation failed before staging publication: {primary}; identity-attested backup rollback failed: {rollback}; staging and any surviving backup are retained"
        )),
    };
    SecureSnapshotPublicationError::before_publication(error)
}

#[cfg(windows)]
fn publish_staging_replacing(
    staging: SecureSnapshotStagingDirectory,
    paths: SecureSnapshotReplacementPaths<'_>,
    original_target: SecureSnapshotSourceTree,
    external_operation_baseline_bytes: usize,
) -> std::result::Result<SecureSnapshotPublishedBackup, SecureSnapshotPublicationError> {
    let SecureSnapshotReplacementPaths {
        requested_target,
        target,
        target_name,
        requested_backup,
        backup,
        backup_name,
    } = paths;
    if relative_entry_exists_nofollow(&staging.parent, Path::new(backup_name))
        .map_err(SecureSnapshotPublicationError::before_publication)?
    {
        return Err(SecureSnapshotPublicationError::before_publication(
            TsinkError::InvalidConfiguration(format!(
                "restore backup destination already exists: {}",
                backup.display()
            )),
        ));
    }
    let operation_live_retained = staging
        .operation_live_retained_bytes()
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    let (target_handle, expected_backup) =
        opened_windows_sibling_directory(&staging.parent, target_name, target)
            .map_err(SecureSnapshotPublicationError::before_publication)?;
    if !expected_backup.stable_eq(original_target.root_identity) {
        return Err(SecureSnapshotPublicationError::before_publication(
            TsinkError::DataCorruption(format!(
                "restore replacement target does not match its pre-move identity manifest: {}",
                target.display()
            )),
        ));
    }
    let SecureSnapshotSourceTree {
        root: original_root,
        root_identity: original_root_identity,
        measurement: original_measurement,
        entries: original_entries,
        ..
    } = original_target;
    drop(target_handle);
    // The original-target root was opened without FILE_SHARE_DELETE. Release only that root
    // handle immediately before the target-to-backup move; the staging parent and its ancestor
    // locks remain retained, and the closed descendant manifest is never recaptured.
    drop(original_root);
    super::rename_path_noreplace(target, backup)
        .map_err(SecureSnapshotPublicationError::before_publication)?;
    let (backup_handle, moved_backup) =
        opened_windows_sibling_directory(&staging.parent, backup_name, backup).map_err(
            |error| {
                windows_prepublication_failure_with_rollback(
                    error,
                    &staging.parent,
                    target,
                    target_name,
                    backup,
                    backup_name,
                    &expected_backup,
                )
            },
        )?;
    if !moved_backup.stable_eq(expected_backup) {
        drop(backup_handle);
        return Err(windows_prepublication_failure_with_rollback(
            TsinkError::DataCorruption(format!(
                "restore target-to-backup move changed directory identity: {}",
                backup.display()
            )),
            &staging.parent,
            target,
            target_name,
            backup,
            backup_name,
            &expected_backup,
        ));
    }
    if let Err(error) = sync_file_as_directory(&staging.parent.root, &staging.parent.display_path) {
        drop(backup_handle);
        return Err(windows_prepublication_failure_with_rollback(
            error,
            &staging.parent,
            target,
            target_name,
            backup,
            backup_name,
            &expected_backup,
        ));
    }

    let expected_root = staging.created[0].identity;
    let expected_created = staging.created;
    let source_path = staging.display_path;
    let parent = staging.parent;
    drop(staging.root);
    if let Err(error) = super::rename_path_noreplace(&source_path, target) {
        drop(backup_handle);
        return Err(windows_prepublication_failure_with_rollback(
            error,
            &parent,
            target,
            target_name,
            backup,
            backup_name,
            &expected_backup,
        ));
    }
    let target_file = open_windows_directory(target)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    let actual_root = identity_from_file(&target_file, target)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    if !actual_root.stable_eq(expected_root) {
        return Err(SecureSnapshotPublicationError::after_publication(
            TsinkError::DataCorruption(format!(
                "Windows restore publication moved an unexpected staging identity to {}",
                target.display()
            )),
        ));
    }
    let published = SecureDirectoryAnchor {
        display_path: target.to_path_buf(),
        root: target_file,
        ancestor_locks: Vec::new(),
    };
    let (observed, observed_bytes) = measure_created_tree(&published, operation_live_retained)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    admit_operation_retained_bytes(
        operation_live_retained,
        observed_bytes,
        "Windows secure restore post-publication verification",
        target,
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    if !created_manifests_match(&expected_created, &observed) {
        return Err(SecureSnapshotPublicationError::after_publication(
            TsinkError::DataCorruption(format!(
                "Windows restore publication tree identity changed at {}",
                target.display()
            )),
        ));
    }
    sync_file_as_directory(&parent.root, &parent.display_path)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    attest_requested_publication_parent(
        &parent,
        requested_target,
        "restore post-publication target-parent attestation",
    )
    .and_then(|()| {
        attest_requested_publication_parent(
            &parent,
            requested_backup,
            "restore post-publication backup-parent attestation",
        )
    })
    .map_err(SecureSnapshotPublicationError::after_publication)?;

    let root = SecureDirectoryAnchor {
        display_path: backup.to_path_buf(),
        root: backup_handle,
        ancestor_locks: Vec::new(),
    };
    let mut published_backup = SecureSnapshotPublishedBackup {
        parent,
        root,
        requested_backup_path: requested_backup.to_path_buf(),
        backup_path: backup.to_path_buf(),
        backup_name: backup_name.to_os_string(),
        root_identity: original_root_identity,
        measurement: original_measurement,
        entries: original_entries,
        retained_memory_bytes: 0,
        external_operation_baseline_bytes,
    };
    published_backup.retained_memory_bytes = published_backup_retained_bytes(&published_backup)
        .map_err(SecureSnapshotPublicationError::after_publication)?;
    admit_operation_retained_bytes(
        published_backup.external_operation_baseline_bytes,
        published_backup.retained_memory_bytes,
        "secure restore published-backup cleanup token",
        backup,
    )
    .map_err(SecureSnapshotPublicationError::after_publication)?;
    Ok(published_backup)
}

#[cfg(windows)]
fn remove_published_backup_exact(mut backup: SecureSnapshotPublishedBackup) -> Result<()> {
    for entry in backup.entries.iter().rev() {
        let expected_parent = *backup.parent_identity(&entry.relative)?;
        let (parent, name, display) =
            open_relative_parent(&backup.root, &entry.relative, &expected_parent)?;
        let probe = open_relative_entry_probe(&backup.root, &entry.relative, &expected_parent)?;
        let metadata = probe_metadata(&probe, &display)?;
        let actual = identity_from_file(&probe.file, &display)?;
        let matches = !is_link_or_reparse_point(&metadata)
            && match entry.kind {
                SecureSourceKind::Directory => {
                    metadata.file_type().is_dir() && actual.stable_eq(entry.identity)
                }
                SecureSourceKind::RegularFile => {
                    metadata.file_type().is_file()
                        && metadata.len() == entry.len
                        && actual == entry.identity
                }
            };
        if !matches {
            return Err(TsinkError::DataCorruption(format!(
                "restore backup entry changed before exact cleanup: {}",
                display.display()
            )));
        }
        // Windows opens snapshot handles without FILE_SHARE_DELETE. Release only the final entry
        // handle immediately before deleting its identity-attested path; retained parent/root
        // handles keep every ancestor namespace pinned.
        drop(probe);
        match entry.kind {
            SecureSourceKind::Directory => std::fs::remove_dir(&display),
            SecureSourceKind::RegularFile => std::fs::remove_file(&display),
        }
        .map_err(|source| TsinkError::IoWithPath {
            path: display.clone(),
            source,
        })?;
        sync_opened_directory(
            &parent,
            backup
                .backup_path
                .join(&entry.relative)
                .parent()
                .unwrap_or(&backup.backup_path),
        )?;
        let _ = name;
    }

    let current_root = identity_from_file(&backup.root.root, &backup.backup_path)?;
    if !current_root.stable_eq(backup.root_identity) {
        return Err(TsinkError::DataCorruption(format!(
            "restore backup root changed before exact cleanup: {}",
            backup.backup_path.display()
        )));
    }
    attest_root_entry(
        &backup.parent,
        &backup.backup_name,
        &backup.backup_path,
        &backup.root_identity,
    )?;
    // The root's no-delete handle is the only remaining handle that blocks removal.
    drop(backup.root);
    std::fs::remove_dir(&backup.backup_path).map_err(|source| TsinkError::IoWithPath {
        path: backup.backup_path.clone(),
        source,
    })?;
    sync_file_as_directory(&backup.parent.root, &backup.parent.display_path)?;
    attest_requested_publication_parent(
        &backup.parent,
        &backup.requested_backup_path,
        "restore post-cleanup backup-parent attestation",
    )
}

#[cfg(windows)]
fn create_relative_directory(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    display: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedDirectory, SecureOpenedDirectory)> {
    let (parent, _name, _) = open_relative_parent(root, relative, expected_parent)?;
    std::fs::create_dir(display).map_err(|source| TsinkError::IoWithPath {
        path: display.to_path_buf(),
        source,
    })?;
    let directory = open_windows_directory(display)?;
    Ok((
        SecureOpenedDirectory {
            file: directory,
            component_locks: Vec::new(),
        },
        parent,
    ))
}

#[cfg(windows)]
fn create_relative_regular_file(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    display: &Path,
    expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedFile, SecureOpenedDirectory)> {
    let (parent, _name, _) = open_relative_parent(root, relative, expected_parent)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .share_mode(WINDOWS_FILE_SHARE_READ)
        .custom_flags(WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT)
        .open(display)
        .map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })?;
    Ok((
        SecureOpenedFile {
            file,
            component_locks: Vec::new(),
        },
        parent,
    ))
}

#[cfg(windows)]
fn relative_entry_exists_nofollow(parent: &SecureDirectoryAnchor, relative: &Path) -> Result<bool> {
    let parent_identity = identity_from_file(&parent.root, &parent.display_path)?;
    match open_relative_entry_probe(parent, relative, &parent_identity) {
        Ok(_) => Ok(true),
        Err(TsinkError::IoWithPath { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

#[cfg(windows)]
fn attest_relative_identity(
    root: &SecureDirectoryAnchor,
    relative: &Path,
    display: &Path,
    kind: SecureSourceKind,
    expected_parent: &ClosedSnapshotIdentity,
    expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    let probe = open_relative_entry_probe(root, relative, expected_parent)?;
    let current = identity_from_file(&probe.file, display)?;
    let matches = match kind {
        SecureSourceKind::Directory => current.stable_eq(*expected),
        SecureSourceKind::RegularFile => &current == expected,
    };
    if !matches {
        return Err(TsinkError::DataCorruption(format!(
            "created staging entry identity changed before attestation: {}",
            display.display()
        )));
    }
    Ok(())
}

#[cfg(windows)]
fn attest_root_entry(
    parent: &SecureDirectoryAnchor,
    root_name: &OsStr,
    display: &Path,
    expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    attest_relative_identity(
        parent,
        Path::new(root_name),
        display,
        SecureSourceKind::Directory,
        &identity_from_file(&parent.root, &parent.display_path)?,
        expected,
    )
}

#[cfg(windows)]
fn for_each_directory_name(
    directory: &SecureOpenedDirectory,
    display: &Path,
    mut visit: impl FnMut(OsString) -> Result<()>,
) -> Result<()> {
    let _locks = &directory.component_locks;
    for entry in std::fs::read_dir(display).map_err(|source| TsinkError::IoWithPath {
        path: display.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })?;
        visit(entry.file_name())?;
    }
    Ok(())
}

#[cfg(windows)]
fn sync_opened_directory(_directory: &SecureOpenedDirectory, display: &Path) -> Result<()> {
    #[cfg(test)]
    invoke_directory_sync_hook(display)?;
    Ok(())
}

#[cfg(windows)]
fn sync_file_as_directory(_file: &File, display: &Path) -> Result<()> {
    #[cfg(test)]
    invoke_directory_sync_hook(display)?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn unsupported_secure_snapshot<T>() -> Result<T> {
    Err(TsinkError::InvalidConfiguration(
        "secure snapshot traversal is unsupported on this platform because no handle-relative no-follow implementation is configured"
            .to_string(),
    ))
}

#[cfg(not(any(unix, windows)))]
fn open_existing_directory_chain_nofollow(
    _path: &Path,
    _operation: &str,
) -> Result<SecureDirectoryAnchor> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn create_directory_chain_nofollow(
    _path: &Path,
    _operation: &str,
) -> Result<SecureDirectoryAnchor> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn open_relative_directory(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedDirectory> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn open_relative_entry_probe(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _expected_parent: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedFile> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn probe_relative_entry_kind_nofollow(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _expected_parent: &ClosedSnapshotIdentity,
) -> Result<ProbedEntryKind> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn open_child_directory(
    _parent: &SecureOpenedDirectory,
    _name: &OsStr,
    _display: &Path,
    _expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedDirectory> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn open_child_entry_probe(
    _parent: &SecureOpenedDirectory,
    _name: &OsStr,
    _display: &Path,
) -> Result<SecureOpenedFile> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn probe_child_entry_kind_nofollow(
    _parent: &SecureOpenedDirectory,
    _name: &OsStr,
    _display: &Path,
) -> Result<ProbedEntryKind> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn open_relative_regular_file(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _expected_parent: &ClosedSnapshotIdentity,
    _expected: &ClosedSnapshotIdentity,
) -> Result<SecureOpenedFile> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn create_staging_at_parent(
    _parent: &SecureDirectoryAnchor,
    _name: &OsStr,
    _display: &Path,
) -> Result<SecureDirectoryAnchor> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn create_relative_directory(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _display: &Path,
    _expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedDirectory, SecureOpenedDirectory)> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn publish_staging_noreplace(
    _staging: SecureSnapshotStagingDirectory,
    _requested_target: &Path,
    _target: &Path,
    _target_name: &OsStr,
) -> std::result::Result<(), SecureSnapshotPublicationError> {
    unsupported_secure_snapshot().map_err(SecureSnapshotPublicationError::before_publication)
}

#[cfg(not(any(unix, windows)))]
fn publish_staging_replacing(
    _staging: SecureSnapshotStagingDirectory,
    _paths: SecureSnapshotReplacementPaths<'_>,
    _original_target: SecureSnapshotSourceTree,
    _external_operation_baseline_bytes: usize,
) -> std::result::Result<SecureSnapshotPublishedBackup, SecureSnapshotPublicationError> {
    unsupported_secure_snapshot().map_err(SecureSnapshotPublicationError::before_publication)
}

#[cfg(not(any(unix, windows)))]
fn remove_published_backup_exact(_backup: SecureSnapshotPublishedBackup) -> Result<()> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn create_relative_regular_file(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _display: &Path,
    _expected_parent: &ClosedSnapshotIdentity,
) -> Result<(SecureOpenedFile, SecureOpenedDirectory)> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn relative_entry_exists_nofollow(
    _parent: &SecureDirectoryAnchor,
    _relative: &Path,
) -> Result<bool> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn attest_relative_identity(
    _root: &SecureDirectoryAnchor,
    _relative: &Path,
    _display: &Path,
    _kind: SecureSourceKind,
    _expected_parent: &ClosedSnapshotIdentity,
    _expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn attest_root_entry(
    _parent: &SecureDirectoryAnchor,
    _root_name: &OsStr,
    _display: &Path,
    _expected: &ClosedSnapshotIdentity,
) -> Result<()> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn for_each_directory_name(
    _directory: &SecureOpenedDirectory,
    _display: &Path,
    _visit: impl FnMut(OsString) -> Result<()>,
) -> Result<()> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn sync_opened_directory(_directory: &SecureOpenedDirectory, _display: &Path) -> Result<()> {
    unsupported_secure_snapshot()
}

#[cfg(not(any(unix, windows)))]
fn sync_file_as_directory(_file: &File, _display: &Path) -> Result<()> {
    unsupported_secure_snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    fn staging_with_file(
        target: &Path,
        relative: &str,
        bytes: &[u8],
    ) -> SecureSnapshotStagingDirectory {
        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(target, "secure-test").unwrap();
        staging
            .write_file(Path::new(relative), bytes, None)
            .unwrap();
        staging.sync_root().unwrap();
        staging
    }

    #[test]
    fn repeated_measurement_of_nonempty_pinned_tree_starts_at_directory_beginning() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("one"), b"one").unwrap();
        std::fs::create_dir(source.join("nested")).unwrap();
        std::fs::write(source.join("nested/two"), b"two").unwrap();

        let tree = SecureSnapshotSourceTree::open_and_measure(&source).unwrap();
        tree.verify_unchanged(tree.retained_memory_bytes()).unwrap();
        tree.verify_unchanged(tree.retained_memory_bytes()).unwrap();
        assert_eq!(tree.measurement().entry_count, 4);
    }

    #[test]
    fn staging_verification_accepts_exact_entry_and_depth_bounds_then_rejects_n_plus_one() {
        let temp = TempDir::new().unwrap();
        let entry_root = temp.path().join("entry-root");
        std::fs::create_dir(&entry_root).unwrap();
        std::fs::create_dir(entry_root.join("empty")).unwrap();
        std::fs::write(entry_root.join("file"), b"x").unwrap();
        let entry_anchor =
            open_existing_directory_chain_nofollow(&entry_root, "test entry root").unwrap();
        let (entries, _) = measure_created_tree_with_limits(&entry_anchor, 0, 3, 8).unwrap();
        assert_eq!(entries.len(), 3);
        std::fs::write(entry_root.join("extra"), b"x").unwrap();
        let entry_err = measure_created_tree_with_limits(&entry_anchor, 0, 3, 8).unwrap_err();
        assert!(entry_err.to_string().contains("exceeds its 3-entry bound"));

        let depth_root = temp.path().join("depth-root");
        std::fs::create_dir_all(depth_root.join("one/two")).unwrap();
        let depth_anchor =
            open_existing_directory_chain_nofollow(&depth_root, "test depth root").unwrap();
        measure_created_tree_with_limits(&depth_anchor, 0, 8, 2).unwrap();
        std::fs::create_dir(depth_root.join("one/two/three")).unwrap();
        let depth_err = measure_created_tree_with_limits(&depth_anchor, 0, 8, 2).unwrap_err();
        assert!(depth_err
            .to_string()
            .contains("directory depth 3 exceeds limit 2"));
    }

    #[test]
    fn secure_copy_and_noreplace_publication_preserve_payload() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("published");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("payload"), b"secure").unwrap();

        let tree = SecureSnapshotSourceTree::open_and_measure(&source).unwrap();
        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(&destination, "snapshot").unwrap();
        staging
            .set_operation_baseline_retained_bytes(tree.retained_memory_bytes(), "test secure copy")
            .unwrap();
        tree.copy_to(&mut staging, Path::new("")).unwrap();
        staging.sync_root().unwrap();
        staging.publish_noreplace(&destination).unwrap();

        assert_eq!(
            std::fs::read(destination.join("payload")).unwrap(),
            b"secure"
        );
    }

    #[test]
    fn validation_copy_cleanup_refreshes_only_known_entry_identities() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(&target, "validation").unwrap();
        let staging_path = staging.path().to_path_buf();
        staging
            .write_file(Path::new("known"), b"before", None)
            .unwrap();
        std::fs::write(staging_path.join("known"), b"after").unwrap();

        staging.remove_after_exact_manifest_refresh().unwrap();
        assert!(!staging_path.exists());
    }

    #[test]
    fn validation_copy_cleanup_refuses_unknown_or_missing_paths_without_deleting_known_files() {
        for mutation in ["unknown", "missing"] {
            let temp = TempDir::new().unwrap();
            let target = temp.path().join("target");
            let mut staging =
                SecureSnapshotStagingDirectory::create_unique(&target, "validation").unwrap();
            let staging_path = staging.path().to_path_buf();
            staging
                .write_file(Path::new("known"), b"owned", None)
                .unwrap();
            match mutation {
                "unknown" => {
                    std::fs::write(staging_path.join("unknown"), b"foreign").unwrap();
                }
                "missing" => {
                    std::fs::remove_file(staging_path.join("known")).unwrap();
                }
                _ => unreachable!(),
            }

            let error = staging
                .remove_after_exact_manifest_refresh()
                .expect_err("changed path set must not be adopted for cleanup");
            assert!(error.to_string().contains("missing, unknown"));
            assert!(staging_path.exists());
            if mutation == "unknown" {
                assert_eq!(std::fs::read(staging_path.join("known")).unwrap(), b"owned");
                assert_eq!(
                    std::fs::read(staging_path.join("unknown")).unwrap(),
                    b"foreign"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn aggregate_namespace_fence_rejects_ancestor_generation_swap_between_sources() {
        let temp = TempDir::new().unwrap();
        let data = temp.path().join("data");
        let moved = temp.path().join("moved-data");
        std::fs::create_dir_all(data.join("numeric")).unwrap();
        std::fs::create_dir_all(data.join("wal")).unwrap();
        std::fs::write(data.join("numeric/segment"), b"old").unwrap();
        let fence = SecureSnapshotNamespaceFence::open_with_operation_baseline(&data, 0).unwrap();
        let numeric = SecureSnapshotSourceTree::open_and_measure_with_operation_baseline(
            &data.join("numeric"),
            fence.retained_memory_bytes(),
        )
        .unwrap();
        assert_eq!(numeric.measurement().entry_count, 2);

        std::fs::rename(&data, &moved).unwrap();
        std::fs::create_dir_all(data.join("wal")).unwrap();
        std::fs::write(data.join("wal/segment"), b"replacement").unwrap();
        let err = fence.attest().unwrap_err();
        assert!(err
            .to_string()
            .contains("aggregate source namespace changed"));
    }

    #[cfg(unix)]
    #[test]
    fn aggregate_namespace_fence_rejects_child_root_swap_between_sources() {
        let temp = TempDir::new().unwrap();
        let data = temp.path().join("data");
        let moved_numeric = data.join("moved-numeric");
        std::fs::create_dir_all(data.join("numeric")).unwrap();
        std::fs::create_dir_all(data.join("wal")).unwrap();
        std::fs::write(data.join("numeric/segment"), b"old").unwrap();
        let fence = SecureSnapshotNamespaceFence::open_with_operation_baseline(&data, 0).unwrap();
        let numeric = SecureSnapshotSourceTree::open_and_measure_with_operation_baseline(
            &data.join("numeric"),
            fence.retained_memory_bytes(),
        )
        .unwrap();
        assert_eq!(numeric.measurement().entry_count, 2);

        std::fs::rename(data.join("numeric"), &moved_numeric).unwrap();
        std::fs::create_dir(data.join("numeric")).unwrap();
        std::fs::write(data.join("numeric/segment"), b"replacement").unwrap();
        let err = fence.attest().unwrap_err();
        assert!(err
            .to_string()
            .contains("aggregate source namespace changed"));
    }

    #[cfg(unix)]
    #[test]
    fn final_component_symlink_swap_during_initial_resolution_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let moved = temp.path().join("moved-source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("payload"), b"original").unwrap();
        let source_for_hook = source.clone();
        let moved_for_hook = moved.clone();
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fired_hook = fired.clone();
        let _hook = install_anchor_resolution_hook(move |candidate| {
            if candidate == source_for_hook.as_path() && !fired_hook.swap(true, Ordering::SeqCst) {
                std::fs::rename(&source_for_hook, &moved_for_hook).unwrap();
                symlink(&moved_for_hook, &source_for_hook).unwrap();
            }
        });

        let err = SecureSnapshotSourceTree::open_and_measure(&source).unwrap_err();
        assert!(fired.load(Ordering::SeqCst));
        assert!(
            err.to_string().contains("symlink")
                || err
                    .to_string()
                    .contains("Too many levels of symbolic links")
                || err.to_string().contains("Not a directory"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn source_ancestor_rename_after_anchor_copies_only_the_pinned_tree() {
        let temp = TempDir::new().unwrap();
        let ancestor = temp.path().join("ancestor");
        let moved = temp.path().join("moved-ancestor");
        let source = ancestor.join("source");
        let destination = temp.path().join("destination");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("payload"), b"pinned").unwrap();
        let tree = SecureSnapshotSourceTree::open_and_measure(&source).unwrap();

        std::fs::rename(&ancestor, &moved).unwrap();
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("payload"), b"replacement").unwrap();

        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(&destination, "snapshot").unwrap();
        staging
            .set_operation_baseline_retained_bytes(
                tree.retained_memory_bytes(),
                "test pinned source",
            )
            .unwrap();
        tree.copy_to(&mut staging, Path::new("")).unwrap();
        assert_eq!(
            std::fs::read(staging.path().join("payload")).unwrap(),
            b"pinned"
        );
        assert_eq!(
            std::fs::read(source.join("payload")).unwrap(),
            b"replacement"
        );
    }

    #[cfg(unix)]
    #[test]
    fn measured_source_child_swapped_to_symlink_is_never_followed() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let original_child = temp.path().join("original-child");
        let outside = temp.path().join("outside");
        let destination = temp.path().join("destination");
        std::fs::create_dir_all(source.join("child")).unwrap();
        std::fs::write(source.join("child/payload"), b"measured").unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("payload"), b"outside-secret").unwrap();
        let tree = SecureSnapshotSourceTree::open_and_measure(&source).unwrap();

        std::fs::rename(source.join("child"), &original_child).unwrap();
        symlink(&outside, source.join("child")).unwrap();
        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(&destination, "snapshot").unwrap();
        staging
            .set_operation_baseline_retained_bytes(
                tree.retained_memory_bytes(),
                "test source child swap",
            )
            .unwrap();
        let err = tree.copy_to(&mut staging, Path::new("")).unwrap_err();
        assert!(
            err.to_string().contains("symlink")
                || err.to_string().contains("Not a directory")
                || err.to_string().contains("Too many levels")
        );
        assert!(!staging.path().join("child/payload").exists());
        assert_eq!(
            std::fs::read(outside.join("payload")).unwrap(),
            b"outside-secret"
        );
    }

    #[test]
    fn same_inode_same_length_source_mutation_is_detected_before_publication() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("payload"), b"AAAA").unwrap();
        let tree = SecureSnapshotSourceTree::open_and_measure(&source).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(source.join("payload"), b"BBBB").unwrap();

        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(&destination, "snapshot").unwrap();
        staging
            .set_operation_baseline_retained_bytes(
                tree.retained_memory_bytes(),
                "test same-object mutation",
            )
            .unwrap();
        let err = tree.copy_to(&mut staging, Path::new("")).unwrap_err();
        assert!(err.to_string().contains("identity changed"));
    }

    #[cfg(unix)]
    #[test]
    fn measurement_rejects_fifo_without_opening_unknown_entry_as_a_file() {
        use std::os::unix::ffi::OsStrExt;

        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let fifo = source.join("fifo");
        let encoded = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);

        let err = SecureSnapshotSourceTree::open_and_measure(&source).unwrap_err();
        assert!(err.to_string().contains("unsupported non-file entry"));
    }

    #[cfg(unix)]
    #[test]
    fn high_entry_measurement_retains_constant_file_descriptor_count() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        for index in 0..2_048 {
            std::fs::write(source.join(format!("entry-{index:04}")), b"x").unwrap();
        }
        let count_fds = || {
            std::fs::read_dir("/dev/fd")
                .or_else(|_| std::fs::read_dir("/proc/self/fd"))
                .unwrap()
                .count()
        };
        let before = count_fds();
        let tree = SecureSnapshotSourceTree::open_and_measure(&source).unwrap();
        let after = count_fds();
        assert_eq!(tree.measurement().entry_count, 2_049);
        assert!(
            after.saturating_sub(before) <= 8,
            "closed-identity manifest retained too many descriptors: before={before}, after={after}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn staging_path_replacement_never_redirects_handle_relative_writes() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("published");
        let mut staging =
            SecureSnapshotStagingDirectory::create_unique(&target, "snapshot").unwrap();
        let original_path = staging.path().to_path_buf();
        let moved_path = temp.path().join("moved-staging");
        std::fs::rename(&original_path, &moved_path).unwrap();
        std::fs::create_dir(&original_path).unwrap();

        staging
            .write_file(Path::new("payload"), b"anchored", None)
            .unwrap();
        assert_eq!(
            std::fs::read(moved_path.join("payload")).unwrap(),
            b"anchored"
        );
        assert!(!original_path.join("payload").exists());
        assert!(staging.verify_exact_created_tree().is_err());
    }

    #[test]
    fn replacement_publication_moves_original_to_backup_and_installs_staging() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let backup = temp.path().join("backup");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old"), b"old").unwrap();
        let original_target = SecureSnapshotSourceTree::open_and_measure(&target).unwrap();
        let mut staging = staging_with_file(&temp.path().join("incoming"), "new", b"new");
        staging
            .set_operation_baseline_retained_bytes(
                original_target.retained_memory_bytes(),
                "replacement test",
            )
            .unwrap();

        let published_backup = staging
            .publish_replacing(&target, &backup, original_target)
            .unwrap();
        assert_eq!(std::fs::read(target.join("new")).unwrap(), b"new");
        assert_eq!(std::fs::read(backup.join("old")).unwrap(), b"old");
        published_backup.remove_and_sync_parent().unwrap();
        assert!(!backup.exists());
    }

    #[test]
    fn replacement_cleanup_rejects_an_unknown_backup_entry_without_deleting_it() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let backup = temp.path().join("backup");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old"), b"old").unwrap();
        let original_target = SecureSnapshotSourceTree::open_and_measure(&target).unwrap();
        let mut staging = staging_with_file(&temp.path().join("incoming"), "new", b"new");
        staging
            .set_operation_baseline_retained_bytes(
                original_target.retained_memory_bytes(),
                "replacement test",
            )
            .unwrap();
        let published_backup = staging
            .publish_replacing(&target, &backup, original_target)
            .unwrap();
        std::fs::write(backup.join("unknown"), b"foreign").unwrap();

        let err = published_backup.remove_and_sync_parent().unwrap_err();
        assert!(err.to_string().contains("pre-move identity manifest"));
        assert_eq!(std::fs::read(target.join("new")).unwrap(), b"new");
        assert_eq!(std::fs::read(backup.join("old")).unwrap(), b"old");
        assert_eq!(std::fs::read(backup.join("unknown")).unwrap(), b"foreign");
    }

    #[test]
    fn replacement_prepublication_sync_failure_rolls_original_target_back() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let backup = temp.path().join("backup");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old"), b"old").unwrap();
        let original_target = SecureSnapshotSourceTree::open_and_measure(&target).unwrap();
        let mut staging = staging_with_file(&temp.path().join("incoming"), "new", b"new");
        staging
            .set_operation_baseline_retained_bytes(
                original_target.retained_memory_bytes(),
                "replacement test",
            )
            .unwrap();
        let staging_path = staging.path().to_path_buf();
        let retained_parent = staging.parent.display_path.clone();
        let _failure = super::super::fail_directory_sync_once(
            retained_parent,
            "injected prepublication parent sync failure",
        );

        let err = staging
            .publish_replacing(&target, &backup, original_target)
            .unwrap_err();
        assert!(!err.published);
        assert_eq!(std::fs::read(target.join("old")).unwrap(), b"old");
        assert!(!backup.exists());
        assert!(staging_path.exists());
    }

    #[test]
    fn replacement_postpublication_sync_failure_retains_visible_target_and_backup() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let backup = temp.path().join("backup");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old"), b"old").unwrap();
        let original_target = SecureSnapshotSourceTree::open_and_measure(&target).unwrap();
        let mut staging = staging_with_file(&temp.path().join("incoming"), "new", b"new");
        staging
            .set_operation_baseline_retained_bytes(
                original_target.retained_memory_bytes(),
                "replacement test",
            )
            .unwrap();
        let parent = staging.parent.display_path.clone();
        let matches = std::sync::Arc::new(AtomicUsize::new(0));
        let match_count = matches.clone();
        let _failure = super::super::fail_directory_sync_matching_once(
            move |candidate| {
                candidate == parent.as_path() && match_count.fetch_add(1, Ordering::SeqCst) == 1
            },
            "injected postpublication parent sync failure",
        );

        let err = staging
            .publish_replacing(&target, &backup, original_target)
            .unwrap_err();
        assert!(err.published);
        assert_eq!(std::fs::read(target.join("new")).unwrap(), b"new");
        assert_eq!(std::fs::read(backup.join("old")).unwrap(), b"old");
    }

    #[test]
    fn noreplace_postpublication_sync_failure_retains_visible_target() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let staging = staging_with_file(&target, "payload", b"visible");
        let retained_parent = staging.parent.display_path.clone();
        let _failure = super::super::fail_directory_sync_once(
            retained_parent,
            "injected snapshot parent sync failure",
        );

        let err = staging.publish_noreplace(&target).unwrap_err();
        assert!(err.published);
        assert_eq!(std::fs::read(target.join("payload")).unwrap(), b"visible");
    }

    #[cfg(unix)]
    #[test]
    fn noreplace_publication_rejects_late_child_created_after_rename() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let staging = staging_with_file(&target, "payload", b"visible");
        let expected_target = target.clone();
        let _late_mutation = install_publication_after_rename_hook(move |published| {
            if published == expected_target {
                std::fs::write(published.join("late-child"), b"late").unwrap();
            }
        });

        let err = staging.publish_noreplace(&target).unwrap_err();
        assert!(err.published);
        assert!(err.error.to_string().contains("tree identity changed"));
        assert_eq!(std::fs::read(target.join("late-child")).unwrap(), b"late");
    }

    #[cfg(unix)]
    #[test]
    fn replacement_publication_rejects_late_same_length_content_mutation() {
        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        let backup = temp.path().join("backup");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old"), b"old").unwrap();
        let original_target = SecureSnapshotSourceTree::open_and_measure(&target).unwrap();
        let mut staging = staging_with_file(&temp.path().join("incoming"), "new", b"new");
        staging
            .set_operation_baseline_retained_bytes(
                original_target.retained_memory_bytes(),
                "replacement test",
            )
            .unwrap();
        let expected_target = target.clone();
        let _late_mutation = install_publication_after_rename_hook(move |published| {
            if published == expected_target {
                std::fs::write(published.join("new"), b"NEW").unwrap();
            }
        });

        let err = staging
            .publish_replacing(&target, &backup, original_target)
            .unwrap_err();
        assert!(err.published);
        assert!(err.error.to_string().contains("tree identity changed"));
        assert_eq!(std::fs::read(target.join("new")).unwrap(), b"NEW");
        assert_eq!(std::fs::read(backup.join("old")).unwrap(), b"old");
    }
}
