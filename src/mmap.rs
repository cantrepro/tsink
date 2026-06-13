//! Architecture-specific memory-mapped file support
//!
//! This module provides platform and architecture-specific memory mapping
//! implementations with appropriate size limits.

use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::io;

/// Maximum map size for different architectures
#[cfg(target_arch = "x86")]
pub const MAX_MAP_SIZE: usize = 0x7FFFFFFF; // 2GB for 32-bit

#[cfg(target_arch = "arm")]
pub const MAX_MAP_SIZE: usize = 0x7FFFFFFF; // 2GB for ARM

#[cfg(target_arch = "x86_64")]
pub const MAX_MAP_SIZE: usize = usize::MAX; // No practical limit on 64-bit

#[cfg(target_arch = "aarch64")]
pub const MAX_MAP_SIZE: usize = usize::MAX; // No practical limit on ARM64

#[cfg(not(any(
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "x86_64",
    target_arch = "aarch64"
)))]
pub const MAX_MAP_SIZE: usize = 0x7FFFFFFF; // Default to 2GB for unknown architectures

/// Platform-specific memory mapping
pub struct PlatformMmap {
    mmap: Mmap,
    #[allow(dead_code)]
    file: File,
}

impl PlatformMmap {
    /// Creates a new memory-mapped file with architecture-specific limits
    pub fn new(file: File, length: usize) -> io::Result<Self> {
        Self::create_map(file, length)
    }

    /// Creates a read-only memory-mapped file
    pub fn new_readonly(file: File, length: usize) -> io::Result<Self> {
        Self::create_map(file, length)
    }

    #[inline]
    fn create_map(file: File, length: usize) -> io::Result<Self> {
        // Only enforce the configured ceiling on platforms where MAX_MAP_SIZE is not unbounded.
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

        Ok(PlatformMmap { mmap, file })
    }

    /// Returns the memory-mapped data as a byte slice
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }

    /// Returns the length of the mapped region
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Checks if the mapped region is empty
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

/// Platform-specific optimizations for Windows
#[cfg(target_os = "windows")]
pub mod windows {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    /// Windows-specific flags for file access
    pub const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x08000000;
    pub const FILE_FLAG_RANDOM_ACCESS: u32 = 0x10000000;

    /// Opens a file optimized for sequential scanning
    pub fn open_sequential(path: &std::path::Path) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
    }

    /// Opens a file optimized for random access
    pub fn open_random(path: &std::path::Path) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_RANDOM_ACCESS)
            .open(path)
    }
}

/// Platform-specific optimizations for Unix-like systems
#[cfg(unix)]
pub mod unix {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    /// Unix-specific flags for file access
    #[cfg(target_os = "linux")]
    pub const O_NOATIME: i32 = 0o1000000;

    #[cfg(not(target_os = "linux"))]
    pub const O_NOATIME: i32 = 0;

    /// Opens a file with platform-specific optimizations
    pub fn open_optimized(path: &std::path::Path) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true);

        #[cfg(target_os = "linux")]
        options.custom_flags(O_NOATIME);

        options.open(path)
    }

    /// Advises the kernel about memory access patterns
    pub fn madvise_sequential(mmap: &Mmap) -> io::Result<()> {
        use libc::{MADV_SEQUENTIAL, madvise};

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

    /// Advises the kernel to expect random access patterns
    pub fn madvise_random(mmap: &Mmap) -> io::Result<()> {
        use libc::{MADV_RANDOM, madvise};

        let ret = unsafe { madvise(mmap.as_ptr() as *mut libc::c_void, mmap.len(), MADV_RANDOM) };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Advises the kernel that we will need this memory soon
    pub fn madvise_willneed(mmap: &Mmap) -> io::Result<()> {
        use libc::{MADV_WILLNEED, madvise};

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

/// Helper function to create an appropriate memory map for the current platform
pub fn create_mmap(file: File) -> io::Result<PlatformMmap> {
    let metadata = file.metadata()?;
    let length = metadata.len() as usize;

    PlatformMmap::new(file, length)
}

/// Helper function to get the maximum safe mmap size for the current platform
pub fn get_max_mmap_size() -> usize {
    MAX_MAP_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_max_map_size() {
        let max_size = get_max_mmap_size();

        #[cfg(target_arch = "x86_64")]
        assert_eq!(max_size, usize::MAX);

        #[cfg(target_arch = "x86")]
        assert_eq!(max_size, 0x7FFFFFFF);
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

    #[test]
    fn test_size_limit() {
        // This should fail on 32-bit architectures if we try to map too much
        #[cfg(all(target_arch = "x86", unix))]
        {
            use std::fs::File;
            let result = PlatformMmap::new(File::open("/dev/zero").unwrap(), MAX_MAP_SIZE + 1);
            assert!(result.is_err());
        }
    }
}
