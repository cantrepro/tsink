//! cgroup support for container-aware resource management
//!
//! This module provides functionality to detect CPU and memory limits
//! within containerized environments by reading cgroup information.

use std::fs;
use std::path::Path;
use std::sync::OnceLock;

static AVAILABLE_CPUS: OnceLock<usize> = OnceLock::new();

/// Returns the number of available CPU cores for the application.
/// This takes into account cgroup CPU quotas if running in a container.
pub fn available_cpus() -> usize {
    *AVAILABLE_CPUS.get_or_init(detect_cpu_quota)
}

/// Detects CPU quota from cgroup settings
fn detect_cpu_quota() -> usize {
    // Allow explicit override for Rust users, with GOMAXPROCS compatibility fallback.
    if let Some(n) =
        parse_cpu_override_env("TSINK_MAX_CPUS").or_else(|| parse_cpu_override_env("GOMAXPROCS"))
    {
        return n;
    }

    // Try to get CPU quota from cgroup
    if let Some(quota) = get_cpu_quota() {
        let num_cpus = num_cpus::get();
        // Respect fractional quotas below 1 CPU by reserving at least one worker.
        let calculated = quota.ceil() as usize;

        if calculated > 0 && calculated < num_cpus {
            return calculated;
        }
    }

    // Fall back to number of logical CPUs
    num_cpus::get()
}

fn parse_cpu_override_env(var_name: &str) -> Option<usize> {
    let value = std::env::var(var_name).ok()?;
    let parsed = value.parse::<usize>().ok()?;
    (parsed > 0).then_some(parsed)
}

/// Gets CPU quota from cgroup v1 or v2
fn get_cpu_quota() -> Option<f64> {
    // Try cgroup v2 first
    if let Some(quota) = get_cpu_quota_v2() {
        return Some(quota);
    }

    // Fall back to cgroup v1
    get_cpu_quota_v1()
}

/// Gets CPU quota from cgroup v2
fn get_cpu_quota_v2() -> Option<f64> {
    let cpu_max_path = "/sys/fs/cgroup/cpu.max";
    if !Path::new(cpu_max_path).exists() {
        return None;
    }

    let content = fs::read_to_string(cpu_max_path).ok()?;
    let parts: Vec<&str> = content.split_whitespace().collect();

    if parts.len() != 2 || parts[0] == "max" {
        return None;
    }

    let quota = parts[0].parse::<f64>().ok()?;
    let period = parts[1].parse::<f64>().ok()?;
    if period <= 0.0 {
        return None;
    }

    Some(quota / period)
}

/// Gets CPU quota from cgroup v1
fn get_cpu_quota_v1() -> Option<f64> {
    let quota = read_cgroup_value("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?;
    if quota <= 0 {
        // Quota not set, check online CPU count
        return get_online_cpu_count();
    }

    let period = read_cgroup_value("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?;
    if period <= 0 {
        return None;
    }
    Some(quota as f64 / period as f64)
}

/// Reads a single integer value from a cgroup file
fn read_cgroup_value(path: &str) -> Option<i64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Gets the count of online CPUs from sysfs
fn get_online_cpu_count() -> Option<f64> {
    let content = fs::read_to_string("/sys/devices/system/cpu/online").ok()?;
    Some(count_cpu_ranges(&content) as f64)
}

/// Counts CPUs from a range string like "0-3,5,7-9"
fn count_cpu_ranges(data: &str) -> usize {
    let data = data.trim();
    let mut count = 0;

    for part in data.split(',') {
        if part.contains('-') {
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() == 2
                && let (Ok(start), Ok(end)) =
                    (bounds[0].parse::<usize>(), bounds[1].parse::<usize>())
                && end >= start
            {
                count += end - start + 1;
            }
        } else if part.parse::<usize>().is_ok() {
            count += 1;
        }
    }

    count
}

/// Returns the memory limit in bytes from cgroup settings
pub fn get_memory_limit() -> Option<i64> {
    // Try cgroup v2 first
    if let Some(limit) = get_memory_limit_v2() {
        return Some(limit);
    }

    // Fall back to cgroup v1
    get_memory_limit_v1()
}

/// Gets memory limit from cgroup v2
fn get_memory_limit_v2() -> Option<i64> {
    let mem_max_path = "/sys/fs/cgroup/memory.max";
    if !Path::new(mem_max_path).exists() {
        return None;
    }

    let content = fs::read_to_string(mem_max_path).ok()?;
    let trimmed = content.trim();

    if trimmed == "max" {
        return None;
    }

    trimmed.parse().ok()
}

/// Gets memory limit from cgroup v1
fn get_memory_limit_v1() -> Option<i64> {
    read_cgroup_value("/sys/fs/cgroup/memory/memory.limit_in_bytes")
}

/// Returns hierarchical memory limit from cgroup v1
pub fn get_hierarchical_memory_limit() -> Option<i64> {
    let stat_path = "/sys/fs/cgroup/memory/memory.stat";
    if !Path::new(stat_path).exists() {
        return None;
    }

    let content = fs::read_to_string(stat_path).ok()?;

    for line in content.lines() {
        if line.starts_with("hierarchical_memory_limit") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                return parts[1].parse().ok();
            }
        }
    }

    None
}

/// Returns the default number of workers based on available CPUs
pub fn default_workers_limit() -> usize {
    available_cpus()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_cpu_ranges() {
        assert_eq!(count_cpu_ranges("0-3"), 4);
        assert_eq!(count_cpu_ranges("0-3,5"), 5);
        assert_eq!(count_cpu_ranges("0-3,5,7-9"), 8);
        assert_eq!(count_cpu_ranges("0"), 1);
        assert_eq!(count_cpu_ranges(""), 0);
        assert_eq!(count_cpu_ranges("3-1"), 0);
    }

    #[test]
    fn test_available_cpus() {
        let cpus = available_cpus();
        assert!(cpus > 0);
        assert!(cpus <= 1024); // Reasonable upper bound
    }
}
