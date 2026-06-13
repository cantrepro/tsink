//! Shared write-ahead log sync policy types.

use std::time::Duration;

/// Sync policy for WAL durability/performance tradeoffs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSyncMode {
    /// Flush and fsync on every append call.
    PerAppend,
    /// Flush every append and fsync at most once per interval.
    Periodic(Duration),
}

impl Default for WalSyncMode {
    fn default() -> Self {
        WalSyncMode::Periodic(Duration::from_secs(1))
    }
}
