//! Shared write-ahead log sync policy types.

use std::time::Duration;

/// Sync policy for WAL durability/performance tradeoffs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSyncMode {
    /// A write is acknowledged only after its WAL frames are flushed and fsync'd.
    #[default]
    PerAppend,
    /// A write is acknowledged after its WAL frames are appended and flushed to the OS, but
    /// before fsync is guaranteed.
    ///
    /// This lowers fsync overhead, but acknowledged writes can still be lost in a crash window
    /// until a later WAL sync or segment flush makes them durable. Callers can observe that
    /// distinction through [`crate::storage::Storage::insert_rows_with_result`].
    Periodic(Duration),
}

impl WalSyncMode {
    pub fn acknowledged_writes_are_durable(self) -> bool {
        matches!(self, Self::PerAppend)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerAppend => "per-append",
            Self::Periodic(_) => "periodic",
        }
    }

    pub fn periodic_interval(self) -> Option<Duration> {
        match self {
            Self::PerAppend => None,
            Self::Periodic(interval) => Some(interval),
        }
    }
}

/// Replay policy when WAL corruption is encountered mid-log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalReplayMode {
    /// Fail replay immediately with a corruption error.
    ///
    /// This is the default for durable startup and helper replays so corruption is surfaced
    /// explicitly instead of silently skipping historical writes.
    #[default]
    Strict,
    /// Skip corrupted frames when boundaries remain trustworthy, otherwise skip the damaged
    /// segment and continue replaying later intact writes.
    Salvage,
}
