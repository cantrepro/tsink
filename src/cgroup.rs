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
    if let Some(n) = parse_cpu_override_env("TSINK_MAX_CPUS") {
        return n;
    }

    if let Some(quota) = get_cpu_quota() {
        let num_cpus = num_cpus::get();
        // Respect fractional quotas below 1 CPU by reserving at least one worker.
        let calculated = quota.ceil() as usize;

        if calculated > 0 && calculated < num_cpus {
            return calculated;
        }
    }

    num_cpus::get()
}

fn parse_cpu_override_env(var_name: &str) -> Option<usize> {
    let value = std::env::var(var_name).ok()?;
    let parsed = value.parse::<usize>().ok()?;
    (parsed > 0).then_some(parsed)
}

/// Gets CPU quota from cgroup v2.
fn get_cpu_quota() -> Option<f64> {
    get_cpu_quota_v2()
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

/// Returns the memory limit in bytes from cgroup settings
pub fn get_memory_limit() -> Option<i64> {
    get_memory_limit_v2()
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

/// Returns the default number of workers based on available CPUs
pub fn default_workers_limit() -> usize {
    available_cpus()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_cpus() {
        let cpus = available_cpus();
        assert!(cpus > 0);
        assert!(cpus <= 1024); // Reasonable upper bound
    }
}
