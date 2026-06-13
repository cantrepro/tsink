use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::Ordering;

use super::super::super::{
    ActiveSeriesState, Chunk, ChunkStorage, MemoryDeltaBytes, Result, SeriesId, SeriesRegistry,
    SeriesResolution, SeriesValueFamily, TsinkError, Value, ValueLane, WalHighWatermark,
    WriteApplyContext, WriteApplyMemoryAccountingContext, WriteApplyPublicationContext,
    WriteApplyRegistryContext, WriteApplyShardMutationContext, WriteApplyWalContext,
    WriteCommitStageContext, WriteCommitWalCompletionContext, WriteSeriesValidationContext,
    IN_MEMORY_SHARD_COUNT,
};
use super::super::lane_name;
use super::phases::{PendingPoint, PreparedWalWrite, StagedWalWrite};
use crate::WriteAcknowledgement;

fn collect_pending_series_lanes(points: &[PendingPoint]) -> Result<BTreeMap<SeriesId, ValueLane>> {
    let mut series_lanes = BTreeMap::new();

    for point in points {
        if let Some(existing_lane) = series_lanes.get(&point.series_id) {
            if *existing_lane != point.lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(*existing_lane).to_string(),
                    actual: lane_name(point.lane).to_string(),
                });
            }
        } else {
            series_lanes.insert(point.series_id, point.lane);
        }
    }

    Ok(series_lanes)
}

impl<'a> WriteSeriesValidationContext<'a> {
    fn validate_series_family_from_registry(
        self,
        series_id: SeriesId,
        family: SeriesValueFamily,
        actual_kind: &str,
    ) -> Result<bool> {
        self.with_registry(|registry| {
            if registry.get_by_id(series_id).is_none() {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} missing from registry during family validation",
                    series_id
                )));
            }

            if let Some(existing_family) = registry.series_value_family(series_id) {
                if existing_family != family {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: existing_family.name().to_string(),
                        actual: actual_kind.to_string(),
                    });
                }
                return Ok(false);
            }

            Ok(true)
        })
    }

    pub(super) fn active_lane(self, series_id: SeriesId) -> Option<ValueLane> {
        self.chunks
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map(|state| state.lane)
    }

    pub(super) fn sealed_lane(self, series_id: SeriesId) -> Option<ValueLane> {
        self.chunks
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .and_then(|chunks| chunks.last_key_value().map(|(_, chunk)| chunk))
            .map(|chunk| chunk.header.lane)
    }

    pub(super) fn persisted_lane(self, series_id: SeriesId) -> Option<ValueLane> {
        self.persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .and_then(|chunks| chunks.last())
            .map(|chunk_ref| chunk_ref.lane)
    }

    pub(super) fn with_active_series<R>(
        self,
        series_id: SeriesId,
        f: impl FnOnce(Option<&ActiveSeriesState>) -> R,
    ) -> R {
        let active = self.chunks.active_shard(series_id).read();
        f(active.get(&series_id))
    }

    pub(super) fn with_registry<R>(self, f: impl FnOnce(&SeriesRegistry) -> R) -> R {
        let registry = self.registry.read();
        f(&registry)
    }
}

impl<'a> WriteApplyRegistryContext<'a> {
    pub(super) fn rollback_created_series(self, created_series: &[SeriesResolution]) {
        if created_series.is_empty() {
            return;
        }

        self.series_validation
            .with_registry(|registry| registry.rollback_created_series(created_series));
        self.registry_memory.sync_registry_memory_usage();
    }

    pub(super) fn reserve_series_value_families(
        self,
        pending_series_families: &BTreeMap<SeriesId, SeriesValueFamily>,
    ) -> Result<Vec<SeriesId>> {
        if pending_series_families.is_empty() {
            return Ok(Vec::new());
        }

        let assigned =
            self.series_validation
                .with_registry(|registry| -> Result<Vec<SeriesId>> {
                    let mut assigned = Vec::new();
                    for (series_id, family) in pending_series_families {
                        match registry.assign_series_value_family_if_missing(*series_id, *family) {
                            Ok(true) => assigned.push(*series_id),
                            Ok(false) => {}
                            Err(err) => {
                                for assigned_series_id in &assigned {
                                    registry.clear_series_value_family(*assigned_series_id);
                                }
                                return Err(err);
                            }
                        }
                    }
                    Ok(assigned)
                })?;
        self.registry_memory.sync_registry_memory_usage();

        Ok(assigned)
    }

    pub(super) fn rollback_series_value_family_reservations(self, series_ids: &[SeriesId]) {
        if series_ids.is_empty() {
            return;
        }

        self.series_validation.with_registry(|registry| {
            for series_id in series_ids {
                registry.clear_series_value_family(*series_id);
            }
        });
        self.registry_memory.sync_registry_memory_usage();
    }
}

impl<'a> WriteApplyWalContext<'a> {
    pub(super) fn abort_staged_wal_write(
        self,
        staged_wal: StagedWalWrite<'a>,
        err: TsinkError,
    ) -> TsinkError {
        match staged_wal.wal_write.abort() {
            Ok(()) => err,
            Err(abort_err) => TsinkError::Wal {
                operation: "abort staged logical write".to_string(),
                details: format!("write failed: {err}; WAL rollback failed: {abort_err}"),
            },
        }
    }

    #[cfg(test)]
    pub(super) fn observe_post_samples_persisted(
        self,
        staged_wal: &mut Option<StagedWalWrite<'a>>,
    ) {
        self.test_hooks.invoke_post_samples_append_hook();
        if self.crash_after_samples_persisted.load(Ordering::SeqCst) {
            if let Some(staged_wal) = staged_wal.take() {
                std::mem::forget(staged_wal.wal_write);
            }
            panic!("simulated crash after WAL persistence before in-memory ingest");
        }
    }
}

impl<'a> WriteApplyMemoryAccountingContext<'a> {
    fn active_state_bytes(self, state: &ActiveSeriesState) -> usize {
        if self.shards.accounting_enabled {
            ChunkStorage::active_state_memory_usage_bytes(state)
        } else {
            0
        }
    }

    fn account_shard_delta(self, shard_idx: usize, delta: MemoryDeltaBytes) {
        self.shards.account_memory_delta(shard_idx, delta);
    }
}

impl<'a> WriteApplyPublicationContext<'a> {
    fn publish_materialized_series_ids(self, materialized_series_ids: BTreeSet<SeriesId>) {
        let inserted_series_ids = self
            .materialized_series
            .insert_materialized_series_ids(materialized_series_ids);
        if inserted_series_ids.is_empty() {
            return;
        }

        self.runtime_metadata_delta
            .reconcile_series_ids(inserted_series_ids.iter().copied());
        self.metadata_shards
            .publish_materialized_series_ids(inserted_series_ids.iter().copied());
    }

    fn publish_finalized_chunks(self, shard_idx: usize, finalized: Vec<(SeriesId, Chunk)>) {
        if finalized.is_empty() {
            return;
        }

        self.sealed_chunks
            .append_finalized_chunks_to_sealed_shard(shard_idx, finalized);
    }

    pub(super) fn mark_series_pending<I>(self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.registry_bookkeeping.mark_series_pending(series_ids);
    }
}

impl<'a> WriteApplyShardMutationContext<'a> {
    pub(super) fn reserve_series_lanes(
        self,
        memory: WriteApplyMemoryAccountingContext<'a>,
        points: &[PendingPoint],
    ) -> Result<Vec<SeriesId>> {
        let series_lanes = collect_pending_series_lanes(points)?;
        let mut lanes_by_shard = BTreeMap::<usize, Vec<(SeriesId, ValueLane)>>::new();
        for (series_id, lane) in series_lanes {
            lanes_by_shard
                .entry(ChunkStorage::series_shard_idx(series_id))
                .or_default()
                .push((series_id, lane));
        }

        let mut shard_guards = Vec::with_capacity(lanes_by_shard.len());
        for (shard_idx, entries) in lanes_by_shard {
            shard_guards.push((
                shard_idx,
                entries,
                self.chunks.active_builders[shard_idx].write(),
            ));
        }

        for (_, entries, active) in &shard_guards {
            for (series_id, lane) in entries {
                if let Some(state) = active.get(series_id) {
                    if state.lane != *lane {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: lane_name(state.lane).to_string(),
                            actual: lane_name(*lane).to_string(),
                        });
                    }
                }
            }
        }

        let mut reserved = Vec::new();
        for (shard_idx, entries, active) in &mut shard_guards {
            let mut shard_delta = MemoryDeltaBytes::default();
            for (series_id, lane) in entries {
                if active.contains_key(series_id) {
                    continue;
                }
                let state = ActiveSeriesState::new(*series_id, *lane, self.chunk_point_cap);
                shard_delta.record_addition(memory.active_state_bytes(&state));
                active.insert(*series_id, state);
                reserved.push(*series_id);
            }
            memory.account_shard_delta(*shard_idx, shard_delta);
        }

        Ok(reserved)
    }

    pub(super) fn rollback_empty_series_lane_reservations(
        self,
        memory: WriteApplyMemoryAccountingContext<'a>,
        series_ids: &[SeriesId],
    ) {
        if series_ids.is_empty() {
            return;
        }

        let mut ids_by_shard = BTreeMap::<usize, Vec<SeriesId>>::new();
        for &series_id in series_ids {
            ids_by_shard
                .entry(ChunkStorage::series_shard_idx(series_id))
                .or_default()
                .push(series_id);
        }

        for (shard_idx, shard_series_ids) in ids_by_shard {
            let mut active = self.chunks.active_builders[shard_idx].write();
            let mut shard_delta = MemoryDeltaBytes::default();
            for series_id in shard_series_ids {
                let should_remove = active
                    .get(&series_id)
                    .is_some_and(ActiveSeriesState::is_empty);
                if should_remove {
                    if let Some(removed) = active.remove(&series_id) {
                        shard_delta.record_removal(memory.active_state_bytes(&removed));
                    }
                }
            }
            memory.account_shard_delta(shard_idx, shard_delta);
        }
    }

    pub(super) fn ingest_pending_points(
        self,
        memory: WriteApplyMemoryAccountingContext<'a>,
        publication: WriteApplyPublicationContext<'a>,
        points: Vec<PendingPoint>,
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        let mut points_by_shard: [Vec<PendingPoint>; IN_MEMORY_SHARD_COUNT] =
            std::array::from_fn(|_| Vec::new());
        for point in points {
            let shard_idx = ChunkStorage::series_shard_idx(point.series_id);
            points_by_shard[shard_idx].push(point);
        }

        for (shard_idx, shard_points) in points_by_shard.into_iter().enumerate() {
            if shard_points.is_empty() {
                continue;
            }
            self.ingest_pending_points_for_shard(memory, publication, shard_idx, shard_points)?;
        }

        Ok(())
    }

    fn ingest_pending_points_for_shard(
        self,
        memory: WriteApplyMemoryAccountingContext<'a>,
        publication: WriteApplyPublicationContext<'a>,
        shard_idx: usize,
        shard_points: Vec<PendingPoint>,
    ) -> Result<()> {
        if shard_points.is_empty() {
            return Ok(());
        }

        let mut finalized = Vec::<(SeriesId, Chunk)>::new();
        let mut materialized_series_ids = BTreeSet::new();
        let mut observed_series_timestamps =
            Vec::<(SeriesId, i64)>::with_capacity(shard_points.len());
        let mut active_delta = MemoryDeltaBytes::default();
        {
            let mut active = self.chunks.active_builders[shard_idx].write();

            for point in shard_points {
                let state_bytes_before = active
                    .get(&point.series_id)
                    .map(|state| memory.active_state_bytes(state))
                    .unwrap_or(0);
                let state = active.entry(point.series_id).or_insert_with(|| {
                    ActiveSeriesState::new(point.series_id, point.lane, self.chunk_point_cap)
                });

                if state.lane != point.lane {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: lane_name(state.lane).to_string(),
                        actual: lane_name(point.lane).to_string(),
                    });
                }
                materialized_series_ids.insert(point.series_id);
                observed_series_timestamps.push((point.series_id, point.ts));

                if let Some(chunk) = state.rotate_partition_if_needed(
                    point.ts,
                    self.partition_window,
                    self.max_active_partition_heads_per_series,
                )? {
                    finalized.push((point.series_id, chunk));
                }

                state.append_point(point.ts, point.value, point.wal_highwater);
                self.timestamps.record_ingested_timestamp(point.ts);

                if let Some(chunk) = state.rotate_full_if_needed()? {
                    finalized.push((point.series_id, chunk));
                }

                active_delta.record_change(state_bytes_before, memory.active_state_bytes(state));
            }

            // Keep the active shard locked until finalized chunks are visible in sealed
            // storage so readers never observe a committed handoff gap.
            publication.publish_finalized_chunks(shard_idx, finalized);
        }
        memory.account_shard_delta(shard_idx, active_delta);

        self.timestamps
            .note_series_timestamps(observed_series_timestamps);
        publication.publish_materialized_series_ids(materialized_series_ids);
        Ok(())
    }
}

impl<'a> WriteApplyContext<'a> {
    pub(in crate::engine::storage_engine) fn append_legacy_point_to_series(
        self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        let family = SeriesValueFamily::from_value(&value, lane)?;
        let family_missing = self
            .series_validation
            .validate_series_family_from_registry(series_id, family, value.kind())?;
        let assigned_series_families = if family_missing {
            self.registry
                .reserve_series_value_families(&BTreeMap::from([(series_id, family)]))?
        } else {
            Vec::new()
        };

        let append_result = self.shard_mutation.ingest_pending_points(
            self.memory,
            self.publication,
            vec![PendingPoint {
                series_id,
                lane,
                ts,
                value,
                wal_highwater: WalHighWatermark::default(),
            }],
        );
        if let Err(err) = append_result {
            self.registry
                .rollback_series_value_family_reservations(&assigned_series_families);
            return Err(err);
        }

        Ok(())
    }
}

impl<'a> WriteCommitStageContext<'a> {
    fn synchronize_series_lanes_before_wal_stage(self, points: &[PendingPoint]) -> Result<()> {
        let series_lanes = collect_pending_series_lanes(points)?;
        if series_lanes.is_empty() {
            return Ok(());
        }

        let mut lanes_by_shard = BTreeMap::<usize, Vec<(SeriesId, ValueLane)>>::new();
        for (series_id, lane) in series_lanes {
            lanes_by_shard
                .entry(ChunkStorage::series_shard_idx(series_id))
                .or_default()
                .push((series_id, lane));
        }

        let mut shard_guards = Vec::with_capacity(lanes_by_shard.len());
        for (shard_idx, entries) in lanes_by_shard {
            shard_guards.push((entries, self.chunks.active_builders[shard_idx].write()));
        }

        for (entries, active) in &shard_guards {
            for (series_id, lane) in entries {
                if let Some(state) = active.get(series_id) {
                    if state.lane != *lane {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: lane_name(state.lane).to_string(),
                            actual: lane_name(*lane).to_string(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) fn stage_wal_write(
        self,
        points: &mut [PendingPoint],
        prepared_wal: Option<&PreparedWalWrite>,
    ) -> Result<Option<StagedWalWrite<'a>>> {
        let Some(prepared_wal) = prepared_wal else {
            return Ok(None);
        };
        let Some(wal) = self.wal else {
            return Ok(None);
        };

        self.synchronize_series_lanes_before_wal_stage(points)?;

        let mut wal_write = match wal.begin_logical_write(prepared_wal.encoded_bytes) {
            Ok(wal_write) => wal_write,
            Err(err) => {
                self.wal_metrics.record_append_error();
                return Err(err);
            }
        };

        for payload in &prepared_wal.encoded_series_definition_payloads {
            if let Err(err) = wal_write.append_series_definition_payload(payload) {
                self.wal_metrics.record_append_error();
                return Err(err);
            }
        }
        #[cfg(test)]
        if !prepared_wal.encoded_series_definition_payloads.is_empty() {
            if let Err(err) = wal_write.flush_pending() {
                self.wal_metrics.record_append_error();
                return Err(err);
            }
            self.test_hooks.invoke_post_series_definitions_append_hook();
        }

        if let Some(samples_payload) = prepared_wal.encoded_samples_payload.as_deref() {
            if let Err(err) = wal_write.append_samples_payload(samples_payload) {
                self.wal_metrics.record_append_error();
                return Err(err);
            }
        }

        let committed_highwater = match wal_write.persist_pending() {
            Ok(highwater) => highwater,
            Err(err) => {
                self.wal_metrics.record_append_error();
                return Err(err);
            }
        };

        if committed_highwater > WalHighWatermark::default() {
            for point in points {
                point.wal_highwater = committed_highwater;
            }
        }

        Ok(Some(StagedWalWrite {
            wal_write,
            committed_highwater,
        }))
    }
}

impl<'a> WriteCommitWalCompletionContext<'a> {
    pub(super) fn publish_wal_write(
        self,
        prepared_wal: Option<&PreparedWalWrite>,
        staged_wal: Option<StagedWalWrite<'a>>,
    ) -> WriteAcknowledgement {
        let mut acknowledgement = WriteAcknowledgement::Volatile;
        #[cfg(test)]
        let mut staged_wal = staged_wal;
        #[cfg(not(test))]
        let staged_wal = staged_wal;

        #[cfg(test)]
        {
            if self.crash_before_publish_persisted.load(Ordering::SeqCst) {
                if let Some(staged_wal) = staged_wal.take() {
                    std::mem::forget(staged_wal.wal_write);
                }
                panic!("simulated crash after in-memory ingest before WAL publish");
            }
        }

        if let (Some(wal), Some(prepared_wal), Some(staged_wal)) =
            (self.wal, prepared_wal, staged_wal)
        {
            self.wal_metrics.record_committed_append(
                prepared_wal.encoded_series_definition_payloads.len(),
                prepared_wal.sample_batch_count,
                prepared_wal.sample_point_count,
                prepared_wal.encoded_bytes,
            );

            match staged_wal.wal_write.publish_persisted() {
                Ok(published_highwater) => {
                    debug_assert_eq!(published_highwater, staged_wal.committed_highwater);
                    acknowledgement =
                        if wal.current_durable_highwater() >= staged_wal.committed_highwater {
                            WriteAcknowledgement::Durable
                        } else {
                            WriteAcknowledgement::Appended
                        };
                }
                Err(err) => {
                    self.observability
                        .record_maintenance_error("publish logical WAL write", &err);
                    tracing::warn!(
                        error = %err,
                        "Committed write failed to advance the WAL publish boundary; returning a volatile acknowledgement"
                    );
                }
            }
        }

        acknowledgement
    }
}
