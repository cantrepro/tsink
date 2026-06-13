//! Platform-aware memory-mapped file helpers.

use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::io;

#[cfg(target_arch = "x86")]
pub const MAX_MAP_SIZE: usize = 0x7FFFFFFF;

#[cfg(target_arch = "arm")]
pub const MAX_MAP_SIZE: usize = 0x7FFFFFFF;

#[cfg(target_arch = "x86_64")]
pub const MAX_MAP_SIZE: usize = usize::MAX;

#[cfg(target_arch = "aarch64")]
pub const MAX_MAP_SIZE: usize = usize::MAX;

#[cfg(not(any(
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "x86_64",
    target_arch = "aarch64"
)))]
pub const MAX_MAP_SIZE: usize = 0x7FFFFFFF;

pub struct PlatformMmap {
    mmap: Mmap,
}

impl PlatformMmap {
    pub fn new(file: File, length: usize) -> io::Result<Self> {
        Self::create_map(file, length)
    }

    pub fn new_readonly(file: File, length: usize) -> io::Result<Self> {
        Self::create_map(file, length)
    }

    #[inline]
    fn create_map(file: File, length: usize) -> io::Result<Self> {
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            if length > MAX_MAP_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Map size {} exceeds maximum {} for this architecture",
                        length, MAX_MAP_SIZE
                    ),
                ));
            }
        }

        let mmap = unsafe { MmapOptions::new().len(length).map(&file)? };
        drop(file);

        Ok(PlatformMmap { mmap })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

#[cfg(target_os = "windows")]
pub mod windows {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    pub const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x08000000;
    pub const FILE_FLAG_RANDOM_ACCESS: u32 = 0x10000000;

    pub fn open_sequential(path: &std::path::Path) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
    }

    pub fn open_random(path: &std::path::Path) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_RANDOM_ACCESS)
            .open(path)
    }
}

#[cfg(unix)]
pub mod unix {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    #[cfg(target_os = "linux")]
    pub const O_NOATIME: i32 = 0o1000000;

    #[cfg(not(target_os = "linux"))]
    pub const O_NOATIME: i32 = 0;

    pub fn open_optimized(path: &std::path::Path) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true);

        #[cfg(target_os = "linux")]
        options.custom_flags(O_NOATIME);

        options.open(path)
    }

    pub fn madvise_sequential(mmap: &Mmap) -> io::Result<()> {
        use libc::{madvise, MADV_SEQUENTIAL};

        let ret = unsafe {
            madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                MADV_SEQUENTIAL,
            )
        };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn madvise_random(mmap: &Mmap) -> io::Result<()> {
        use libc::{madvise, MADV_RANDOM};

        let ret = unsafe { madvise(mmap.as_ptr() as *mut libc::c_void, mmap.len(), MADV_RANDOM) };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn madvise_willneed(mmap: &Mmap) -> io::Result<()> {
        use libc::{madvise, MADV_WILLNEED};

        let ret = unsafe {
            madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                MADV_WILLNEED,
            )
        };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

pub fn create_mmap(file: File) -> io::Result<PlatformMmap> {
    let metadata = file.metadata()?;
    let file_len = metadata.len();
    let length = usize::try_from(file_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "file length {} exceeds usize::MAX ({}) on this platform",
                file_len,
                usize::MAX
            ),
        )
    })?;

    PlatformMmap::new(file, length)
}

pub fn get_max_mmap_size() -> usize {
    MAX_MAP_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    use tempfile::NamedTempFile;

    #[test]
    fn test_max_map_size() {
        #[cfg(target_arch = "x86_64")]
        assert_eq!(get_max_mmap_size(), usize::MAX);

        #[cfg(target_arch = "x86")]
        assert_eq!(get_max_mmap_size(), 0x7FFFFFFF);
    }

    #[test]
    fn test_platform_mmap() -> io::Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        let data = b"Hello, memory-mapped world!";
        temp_file.write_all(data)?;
        temp_file.flush()?;

        let file = temp_file.reopen()?;
        let mmap = PlatformMmap::new(file, data.len())?;

        assert_eq!(mmap.len(), data.len());
        assert_eq!(mmap.as_slice(), data);

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_platform_mmap_does_not_retain_file_descriptor() -> io::Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        let data = b"fd-leak-regression";
        temp_file.write_all(data)?;
        temp_file.flush()?;

        let file = temp_file.reopen()?;
        let reopened_fd = file.as_raw_fd();
        let reopened_fd_path = format!("/proc/self/fd/{reopened_fd}");
        let reopened_target = fs::read_link(&reopened_fd_path)?;
        let mmap = PlatformMmap::new(file, data.len())?;

        assert_eq!(mmap.as_slice(), data);
        if let Ok(current_target) = fs::read_link(&reopened_fd_path) {
            assert_ne!(
                current_target, reopened_target,
                "memory-mapped segments should not retain an open file descriptor"
            );
        }

        Ok(())
    }

    #[test]
    fn test_size_limit() {
        #[cfg(all(target_arch = "x86", unix))]
        {
            use std::fs::File;
            let result = PlatformMmap::new(File::open("/dev/zero").unwrap(), MAX_MAP_SIZE + 1);
            assert!(result.is_err());
        }
    }
}
