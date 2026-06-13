use super::planning::StartupPlan;
use super::*;
use crate::engine::fs_utils::remove_path_if_exists;

pub(super) struct StartupWalOpenPhase;

impl StartupWalOpenPhase {
    pub(super) fn open(
        builder: &StorageBuilder,
        plan: &StartupPlan,
        replay_highwater: WalHighWatermark,
    ) -> Result<Option<FramedWal>> {
        let Some(wal_path) = plan.paths().wal_path.clone() else {
            return Ok(None);
        };

        if plan.wal_enabled() {
            let wal = FramedWal::open_with_buffer_size(
                wal_path,
                builder.wal_sync_mode(),
                builder.wal_buffer_size(),
            )?;
            wal.ensure_min_highwater(replay_highwater)?;
            Ok(Some(wal))
        } else {
            remove_path_if_exists(&wal_path)?;
            Ok(None)
        }
    }
}
