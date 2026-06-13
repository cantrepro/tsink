use super::super::super::{Result, WriteCommitContext};
use super::apply::WriteApplier;
use super::phases::{
    AppliedWrite, CommittedWrite, PreparedWrite, StagePreparedWriteError, StagedWrite,
};

pub(super) struct WriteCommitter<'a> {
    engine: WriteCommitContext<'a>,
}

impl<'a> WriteCommitter<'a> {
    pub(super) fn new(engine: WriteCommitContext<'a>) -> Self {
        Self { engine }
    }

    pub(super) fn stage_prepared_write(
        &self,
        mut prepared: PreparedWrite,
    ) -> std::result::Result<StagedWrite<'a>, StagePreparedWriteError> {
        let staged_wal = match self.engine.stage.stage_wal_write(
            &mut prepared.resolved.pending_points,
            prepared.prepared_wal.as_ref(),
        ) {
            Ok(staged_wal) => staged_wal,
            Err(err) => return Err(Box::new((prepared, err))),
        };

        Ok(StagedWrite {
            prepared,
            staged_wal,
            reserved_series: Vec::new(),
            assigned_series_families: Vec::new(),
        })
    }

    pub(super) fn stage_prepared_write_or_rollback(
        &self,
        applier: &WriteApplier<'a>,
        prepared: PreparedWrite,
    ) -> Result<StagedWrite<'a>> {
        match self.stage_prepared_write(prepared) {
            Ok(staged) => Ok(staged),
            Err(err) => {
                let (prepared, err) = *err;
                Err(applier.rollback_prepared_write_error(prepared, err))
            }
        }
    }

    pub(super) fn publish_applied_write(&self, mut applied: AppliedWrite<'a>) -> CommittedWrite {
        CommittedWrite {
            acknowledgement: self
                .engine
                .wal_completion
                .publish_wal_write(applied.prepared_wal.as_ref(), applied.staged_wal.take()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::prepare::WritePreparer;
    use super::super::resolve::WriteResolver;
    use super::*;
    use crate::engine::storage_engine::{ChunkStorage, WAL_DIR_NAME};
    use crate::engine::wal::FramedWal;
    use crate::wal::WalSyncMode;
    use crate::{DataPoint, Label, Row, Storage, WriteAcknowledgement};
    use tempfile::TempDir;

    #[test]
    fn publish_phase_is_the_only_step_that_advances_committed_visibility() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new(2, Some(wal));
        let labels = vec![Label::new("host", "a")];
        let metric = "phase_boundary_metric";

        let resolver = WriteResolver::new(storage.write_resolve_context());
        let preparer = WritePreparer::new(storage.write_prepare_context());
        let applier = WriteApplier::new(storage.write_apply_context());
        let committer = WriteCommitter::new(storage.write_commit_context());

        let resolved = resolver
            .resolve_write_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        let prepared = preparer
            .prepare_resolved_write_or_rollback(&resolver, resolved)
            .expect("prepare should succeed");
        let staged = committer
            .stage_prepared_write_or_rollback(&applier, prepared)
            .expect("stage should succeed");

        assert!(storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .replay_committed_writes()
            .unwrap()
            .is_empty());

        let applied = match applier.apply_staged_write_or_rollback(staged) {
            Ok(applied) => applied,
            Err(_) => panic!("apply should succeed before publish"),
        };
        assert_eq!(
            storage.select(metric, &labels, 0, 10).unwrap(),
            vec![DataPoint::new(1, 1.0)]
        );
        assert!(
            storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_committed_writes()
                .unwrap()
                .is_empty(),
            "applied-but-unpublished writes must stay behind the logical WAL boundary"
        );
        assert!(
            !storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_frames()
                .unwrap()
                .is_empty(),
            "the stage phase should persist WAL bytes before publish"
        );

        let committed = committer.publish_applied_write(applied);
        assert!(matches!(
            committed.acknowledgement,
            WriteAcknowledgement::Durable | WriteAcknowledgement::Appended
        ));
        assert_eq!(
            storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_committed_writes()
                .unwrap()
                .len(),
            1
        );
    }
}
