use std::collections::BTreeMap;

use super::super::super::{
    Result, SamplesBatchFrame, SeriesId, SeriesValueFamily, TsinkError, Value, ValueLane,
    WriteApplyContext, WriteSeriesValidationContext,
};
use super::super::lane_name;
use super::phases::{
    AppliedWrite, ApplyStagedWriteError, PendingPoint, PreparedWrite, StagedWrite,
};

pub(super) struct WriteApplier<'a> {
    engine: WriteApplyContext<'a>,
}

impl<'a> WriteApplier<'a> {
    pub(super) fn new(engine: WriteApplyContext<'a>) -> Self {
        Self { engine }
    }

    pub(super) fn append_point_to_series(
        &self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        self.engine
            .append_legacy_point_to_series(series_id, lane, ts, value)
    }

    pub(super) fn rollback_prepared_write(&self, prepared: PreparedWrite) {
        self.engine
            .registry
            .rollback_created_series(&prepared.resolved.created_series);
    }

    pub(super) fn rollback_prepared_write_error(
        &self,
        prepared: PreparedWrite,
        err: TsinkError,
    ) -> TsinkError {
        self.rollback_prepared_write(prepared);
        err
    }

    pub(super) fn rollback_staged_write(
        &self,
        staged: StagedWrite<'a>,
        err: TsinkError,
    ) -> TsinkError {
        let StagedWrite {
            prepared,
            staged_wal,
            reserved_series,
            assigned_series_families,
        } = staged;
        let err = if let Some(staged_wal) = staged_wal {
            self.engine.wal.abort_staged_wal_write(staged_wal, err)
        } else {
            err
        };
        self.engine
            .registry
            .rollback_series_value_family_reservations(&assigned_series_families);
        self.engine
            .shard_mutation
            .rollback_empty_series_lane_reservations(self.engine.memory, &reserved_series);
        self.rollback_prepared_write(prepared);
        err
    }

    pub(super) fn apply_staged_write(
        &self,
        mut staged: StagedWrite<'a>,
    ) -> std::result::Result<AppliedWrite<'a>, ApplyStagedWriteError<'a>> {
        staged.reserved_series = match self
            .engine
            .shard_mutation
            .reserve_series_lanes(self.engine.memory, &staged.prepared.resolved.pending_points)
        {
            Ok(reserved_series) => reserved_series,
            Err(err) => return Err(Box::new((staged, err))),
        };

        #[cfg(test)]
        if staged.prepared.prepared_wal.is_some() {
            self.engine
                .wal
                .observe_post_samples_persisted(&mut staged.staged_wal);
        }

        staged.assigned_series_families = match self
            .engine
            .registry
            .reserve_series_value_families(&staged.prepared.pending_series_families)
        {
            Ok(assigned_series_families) => assigned_series_families,
            Err(err) => return Err(Box::new((staged, err))),
        };

        if let Err(err) = self.engine.shard_mutation.ingest_pending_points(
            self.engine.memory,
            self.engine.publication,
            std::mem::take(&mut staged.prepared.resolved.pending_points),
        ) {
            return Err(Box::new((staged, err)));
        }

        self.engine.publication.mark_series_pending(
            staged
                .prepared
                .resolved
                .created_series
                .iter()
                .map(|resolution| resolution.series_id),
        );

        let StagedWrite {
            prepared,
            staged_wal,
            ..
        } = staged;
        Ok(AppliedWrite {
            prepared_wal: prepared.prepared_wal,
            staged_wal,
        })
    }

    pub(super) fn apply_staged_write_or_rollback(
        &self,
        staged: StagedWrite<'a>,
    ) -> Result<AppliedWrite<'a>> {
        match self.apply_staged_write(staged) {
            Ok(applied) => Ok(applied),
            Err(err) => {
                let (staged, err) = *err;
                Err(self.rollback_staged_write(staged, err))
            }
        }
    }

    pub(super) fn validate_series_lane_compatible(
        engine: WriteSeriesValidationContext<'a>,
        series_id: SeriesId,
        lane: ValueLane,
    ) -> Result<()> {
        if let Some(active_lane) = engine.active_lane(series_id) {
            if active_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(active_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        if let Some(sealed_lane) = engine.sealed_lane(series_id) {
            if sealed_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(sealed_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        if let Some(persisted_lane) = engine.persisted_lane(series_id) {
            if persisted_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(persisted_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        Ok(())
    }

    pub(super) fn group_pending_point_indexes_by_series(
        points: &[PendingPoint],
    ) -> Result<BTreeMap<SeriesId, (ValueLane, Vec<usize>)>> {
        let mut grouped: BTreeMap<SeriesId, (ValueLane, Vec<usize>)> = BTreeMap::new();

        for (idx, point) in points.iter().enumerate() {
            let entry = grouped
                .entry(point.series_id)
                .or_insert_with(|| (point.lane, Vec::new()));

            if entry.0 != point.lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(entry.0).to_string(),
                    actual: lane_name(point.lane).to_string(),
                });
            }

            entry.1.push(idx);
        }

        Ok(grouped)
    }

    pub(super) fn validate_pending_point_families(
        engine: WriteSeriesValidationContext<'a>,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<BTreeMap<SeriesId, SeriesValueFamily>> {
        let mut pending_series_families = BTreeMap::new();
        engine.with_registry(|registry| -> Result<()> {
            for (series_id, (lane, indexes)) in grouped {
                let Some((&first_idx, remaining)) = indexes.split_first() else {
                    continue;
                };

                let first_point = &points[first_idx];
                let first_family = SeriesValueFamily::from_value(&first_point.value, *lane)?;

                if registry.get_by_id(*series_id).is_none() {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} missing from registry during family validation",
                        series_id
                    )));
                }
                let registered_family = registry.series_value_family(*series_id);

                for idx in remaining {
                    let point = &points[*idx];
                    let family = SeriesValueFamily::from_value(&point.value, *lane)?;
                    if family != first_family {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: first_family.name().to_string(),
                            actual: point.value.kind().to_string(),
                        });
                    }
                }

                if let Some(existing_family) = registered_family {
                    if existing_family != first_family {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: existing_family.name().to_string(),
                            actual: first_point.value.kind().to_string(),
                        });
                    }
                    continue;
                }

                engine.with_active_series(*series_id, |state| -> Result<()> {
                    if let Some(state) = state {
                        for existing_point in state.points_in_partition_order() {
                            let existing_family =
                                SeriesValueFamily::from_value(&existing_point.value, *lane)?;
                            if existing_family != first_family {
                                return Err(TsinkError::ValueTypeMismatch {
                                    expected: existing_family.name().to_string(),
                                    actual: first_point.value.kind().to_string(),
                                });
                            }
                        }
                    }
                    Ok(())
                })?;

                pending_series_families.insert(*series_id, first_family);
            }
            Ok(())
        })?;

        Ok(pending_series_families)
    }

    pub(super) fn encode_wal_batches(
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<Vec<SamplesBatchFrame>> {
        let mut batches = Vec::with_capacity(grouped.len());
        for (series_id, (lane, indexes)) in grouped {
            let mut point_refs = Vec::with_capacity(indexes.len());
            for idx in indexes {
                let point = &points[*idx];
                point_refs.push((point.ts, &point.value));
            }

            batches.push(SamplesBatchFrame::from_timestamp_value_refs(
                *series_id,
                *lane,
                &point_refs,
            )?);
        }

        Ok(batches)
    }

    pub(super) fn ingest_replayed_pending_points(
        &self,
        pending_points: Vec<PendingPoint>,
    ) -> Result<()> {
        if pending_points.is_empty() {
            return Ok(());
        }

        let grouped_points = Self::group_pending_point_indexes_by_series(&pending_points)?;
        for (series_id, (lane, _)) in &grouped_points {
            Self::validate_series_lane_compatible(
                self.engine.series_validation,
                *series_id,
                *lane,
            )?;
        }
        let pending_series_families = Self::validate_pending_point_families(
            self.engine.series_validation,
            &pending_points,
            &grouped_points,
        )?;
        let reserved_series = self
            .engine
            .shard_mutation
            .reserve_series_lanes(self.engine.memory, &pending_points)?;
        // Recovery is replaying already-committed WAL writes, so it must rebuild the in-memory
        // state without re-running admission-control or retention rejections from live ingest.
        let assigned_series_families = match self
            .engine
            .registry
            .reserve_series_value_families(&pending_series_families)
        {
            Ok(assigned_series_families) => assigned_series_families,
            Err(err) => {
                self.engine
                    .shard_mutation
                    .rollback_empty_series_lane_reservations(self.engine.memory, &reserved_series);
                return Err(err);
            }
        };

        if let Err(err) = self.engine.shard_mutation.ingest_pending_points(
            self.engine.memory,
            self.engine.publication,
            pending_points,
        ) {
            self.engine
                .registry
                .rollback_series_value_family_reservations(&assigned_series_families);
            self.engine
                .shard_mutation
                .rollback_empty_series_lane_reservations(self.engine.memory, &reserved_series);
            return Err(err);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::commit::WriteCommitter;
    use super::super::prepare::WritePreparer;
    use super::super::resolve::WriteResolver;
    use super::*;
    use crate::engine::storage_engine::{ChunkStorage, ChunkStorageOptions, WAL_DIR_NAME};
    use crate::engine::wal::{FramedWal, ReplayFrame};
    use crate::wal::WalSyncMode;
    use crate::{DataPoint, Label, Row, Storage};
    use tempfile::TempDir;

    fn assert_engine_memory_usage_reconciled(storage: &ChunkStorage) {
        if !storage.memory.accounting_enabled {
            return;
        }

        let incremental = storage.memory_observability_snapshot();
        let reconciled_total = storage.refresh_memory_usage();
        let reconciled = storage.memory_observability_snapshot();

        assert_eq!(incremental.budgeted_bytes, reconciled_total);
        assert_eq!(incremental.budgeted_bytes, reconciled.budgeted_bytes);
        assert_eq!(incremental.excluded_bytes, reconciled.excluded_bytes);
        assert_eq!(
            incremental.active_and_sealed_bytes,
            reconciled.active_and_sealed_bytes
        );
        assert_eq!(incremental.registry_bytes, reconciled.registry_bytes);
        assert_eq!(
            incremental.metadata_cache_bytes,
            reconciled.metadata_cache_bytes
        );
        assert_eq!(
            incremental.persisted_index_bytes,
            reconciled.persisted_index_bytes
        );
        assert_eq!(
            incremental.persisted_mmap_bytes,
            reconciled.persisted_mmap_bytes
        );
        assert_eq!(incremental.tombstone_bytes, reconciled.tombstone_bytes);
        assert_eq!(
            incremental.excluded_persisted_mmap_bytes,
            reconciled.excluded_persisted_mmap_bytes
        );
    }

    #[test]
    fn staged_rollback_aborts_wal_bytes_before_error_returns() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            None,
            None,
            1,
            ChunkStorageOptions {
                memory_budget_bytes: 1_000_000,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        let labels = vec![Label::new("host", "a")];
        let metric = "staged_rollback_metric";

        storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let sample_batches_before: usize = storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .replay_frames()
            .unwrap()
            .into_iter()
            .map(|frame| match frame {
                ReplayFrame::Samples(batches) => batches.len(),
                ReplayFrame::SeriesDefinition(_) => 0,
            })
            .sum();

        let resolver = WriteResolver::new(storage.write_resolve_context());
        let preparer = WritePreparer::new(storage.write_prepare_context());
        let applier = WriteApplier::new(storage.write_apply_context());
        let committer = WriteCommitter::new(storage.write_commit_context());

        let resolved = resolver
            .resolve_write_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(2, 2.0),
            )])
            .unwrap();
        let prepared = preparer
            .prepare_resolved_write(resolved)
            .expect("prepare should succeed");
        let staged = committer
            .stage_prepared_write(prepared)
            .expect("stage should succeed");

        let series_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing(metric, &labels)
            .unwrap()
            .series_id;
        storage
            .active_shard(series_id)
            .write()
            .get_mut(&series_id)
            .unwrap()
            .lane = ValueLane::Blob;

        let err = match applier.apply_staged_write_or_rollback(staged) {
            Ok(_) => panic!("apply should fail after the injected lane drift"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            TsinkError::ValueTypeMismatch { expected, actual }
                if expected == "blob" && actual == "numeric"
        ));

        let sample_batches_after: usize = storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .replay_frames()
            .unwrap()
            .into_iter()
            .map(|frame| match frame {
                ReplayFrame::Samples(batches) => batches.len(),
                ReplayFrame::SeriesDefinition(_) => 0,
            })
            .sum();
        assert_eq!(sample_batches_after, sample_batches_before);
        assert_engine_memory_usage_reconciled(&storage);
    }
}
