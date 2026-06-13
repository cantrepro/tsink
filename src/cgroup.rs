//! Container-aware CPU and memory limit helpers.

use std::fs;
use std::path::Path;
use std::sync::OnceLock;

static AVAILABLE_CPUS: OnceLock<usize> = OnceLock::new();

/// Returns the CPU budget visible to the current process, including cgroup limits.
pub fn available_cpus() -> usize {
    *AVAILABLE_CPUS.get_or_init(detect_cpu_quota)
}

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

fn get_cpu_quota() -> Option<f64> {
    get_cpu_quota_unified()
}

fn get_cpu_quota_unified() -> Option<f64> {
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

pub fn get_memory_limit() -> Option<u64> {
    get_memory_limit_unified()
}

fn get_memory_limit_unified() -> Option<u64> {
    let mem_max_path = "/sys/fs/cgroup/memory.max";
    if !Path::new(mem_max_path).exists() {
        return None;
    }

    let content = fs::read_to_string(mem_max_path).ok()?;
    let trimmed = content.trim();

    if trimmed == "max" {
        return None;
    }

    trimmed.parse::<u64>().ok()
}

pub fn default_workers_limit() -> usize {
    available_cpus()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(unused_unsafe)]
    fn set_env_var(key: &str, value: &str) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    #[allow(unused_unsafe)]
    fn remove_env_var(key: &str) {
        unsafe {
            std::env::remove_var(key);
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn new(key: &'static str, value: Option<&str>) -> Self {
            let previous = std::env::var(key).ok();
            match value {
                Some(v) => set_env_var(key, v),
                None => remove_env_var(key),
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.previous.as_deref() {
                Some(v) => set_env_var(self.key, v),
                None => remove_env_var(self.key),
            }
        }
    }

    #[test]
    fn test_available_cpus() {
        let cpus = available_cpus();
        assert!(cpus > 0);
        assert!(cpus <= 1024);
    }

    #[test]
    fn parse_cpu_override_env_accepts_positive_usize() {
        let key = "TSINK_TEST_CPU_OVERRIDE_VALID";
        let _guard = EnvVarGuard::new(key, Some("7"));

        assert_eq!(parse_cpu_override_env(key), Some(7));
    }

    #[test]
    fn parse_cpu_override_env_rejects_zero() {
        let key = "TSINK_TEST_CPU_OVERRIDE_ZERO";
        let _guard = EnvVarGuard::new(key, Some("0"));

        assert_eq!(parse_cpu_override_env(key), None);
    }

    #[test]
    fn parse_cpu_override_env_rejects_non_numeric_values() {
        let key = "TSINK_TEST_CPU_OVERRIDE_INVALID";
        let _guard = EnvVarGuard::new(key, Some("not-a-number"));

        assert_eq!(parse_cpu_override_env(key), None);
    }

    #[test]
    fn parse_cpu_override_env_returns_none_when_missing() {
        let key = "TSINK_TEST_CPU_OVERRIDE_MISSING";
        let _guard = EnvVarGuard::new(key, None);

        assert_eq!(parse_cpu_override_env(key), None);
    }

    #[test]
    fn parse_cpu_override_env_rejects_values_larger_than_usize() {
        let key = "TSINK_TEST_CPU_OVERRIDE_OVERFLOW";
        let _guard = EnvVarGuard::new(key, Some("18446744073709551616"));

        assert_eq!(parse_cpu_override_env(key), None);
    }

    #[test]
    fn default_workers_limit_uses_available_cpu_count() {
        assert_eq!(default_workers_limit(), available_cpus());
    }
}
