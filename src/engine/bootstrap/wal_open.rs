use super::planning::StartupPlan;
use super::*;
use crate::engine::fs_utils::remove_path_if_exists;

pub(super) struct StartupWalOpenPhase;

impl StartupWalOpenPhase {
    pub(super) fn open(
        builder: &StorageBuilder,
        plan: &StartupPlan,
        replay_highwater: WalHighWatermark,
        snapshot_validation: bool,
    ) -> Result<Option<FramedWal>> {
        let Some(wal_path) = plan.paths().wal_path.clone() else {
            return Ok(None);
        };

        if plan.wal_enabled() {
            let wal =
                FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode_and_creation(
                    wal_path,
                    builder.wal_sync_mode(),
                    builder.wal_buffer_size(),
                    plan.local_disk_budget().cloned(),
                    builder.wal_replay_mode(),
                    !snapshot_validation,
                )?;
            if snapshot_validation {
                wal.ensure_min_highwater_without_segment_creation(replay_highwater)?;
            } else {
                wal.ensure_min_highwater(replay_highwater)?;
            }
            Ok(Some(wal))
        } else {
            remove_path_if_exists(&wal_path)?;
            Ok(None)
        }
    }
}
