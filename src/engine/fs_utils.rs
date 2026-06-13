use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Result, TsinkError};

static STAGE_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(crate) fn remove_dir_if_exists(path: &Path) -> std::io::Result<bool> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn remove_file_if_exists(path: &Path) -> std::io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn remove_path_if_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        let _ = remove_dir_if_exists(path)?;
    } else {
        let _ = remove_file_if_exists(path)?;
    }

    Ok(())
}

pub(crate) fn remove_path_if_exists_and_sync_parent(path: &Path) -> Result<()> {
    let existed = path.exists();
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
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(TsinkError::Other(format!(
        "failed to allocate unique staging path for {}",
        target.display()
    )))
}

pub(crate) fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::metadata(source)?;
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
    match std::fs::metadata(source) {
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
    let metadata = std::fs::metadata(source)?;
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

pub(crate) fn tmp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    path.with_file_name(format!("{file_name}.tmp"))
}

pub(crate) fn write_tmp_and_sync(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = tmp_path_for(path);
    let file = std::fs::File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

pub(crate) fn rename_tmp(path: &Path) -> Result<()> {
    std::fs::rename(tmp_path_for(path), path)?;
    Ok(())
}

pub(crate) fn rename_and_sync_parents(source: &Path, destination: &Path) -> Result<()> {
    std::fs::rename(source, destination)?;

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

pub(crate) fn sync_dir_if_exists(path: &Path) {
    let _ = sync_dir_if_exists_result(path);
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    let dir = std::fs::File::open(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })
}

pub(crate) fn sync_dir_if_exists_result(path: &Path) -> Result<()> {
    match std::fs::File::open(path) {
        Ok(dir) => dir.sync_all().map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn write_file_atomically_and_sync_parent(path: &Path, bytes: &[u8]) -> Result<()> {
    write_tmp_and_sync(path, bytes)?;
    rename_tmp(path)?;
    if let Some(parent) = path.parent() {
        sync_dir_if_exists(parent);
    }
    Ok(())
}
