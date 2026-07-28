use super::analysis::{analyze_series_read_sources, SeriesReadAnalysis};
use super::*;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct MergePathShardSnapshotStats {
    snapshots: u64,
    wait_nanos: u64,
    hold_nanos: u64,
}

impl MergePathShardSnapshotStats {
    pub(super) fn single(wait_nanos: u64, hold_nanos: u64) -> Self {
        Self {
            snapshots: 1,
            wait_nanos,
            hold_nanos,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_count(self) -> u64 {
        self.snapshots
    }
}

#[derive(Default)]
pub(super) struct PersistedSeriesSourceSnapshot {
    pub(super) chunks: Vec<PersistedChunkRef>,
    pub(super) segment_maps: HashMap<usize, Arc<PlatformMmap>>,
    pub(super) segment_tiers: HashMap<usize, PersistedSegmentTier>,
    _query_reservation: Option<crate::QueryMemoryReservation>,
}

pub(super) struct PersistedSeriesSourceSnapshotParts {
    pub(super) chunks: Vec<PersistedChunkRef>,
    pub(super) segment_maps: HashMap<usize, Arc<PlatformMmap>>,
    pub(super) segment_tiers: HashMap<usize, PersistedSegmentTier>,
    pub(super) query_reservation: Option<crate::QueryMemoryReservation>,
}

impl PersistedSeriesSourceSnapshot {
    pub(super) fn chunk_tier(&self, chunk_ref: &PersistedChunkRef) -> PersistedSegmentTier {
        self.segment_tiers
            .get(&chunk_ref.segment_slot)
            .copied()
            .unwrap_or(PersistedSegmentTier::Hot)
    }

    pub(super) fn into_parts(self) -> PersistedSeriesSourceSnapshotParts {
        PersistedSeriesSourceSnapshotParts {
            chunks: self.chunks,
            segment_maps: self.segment_maps,
            segment_tiers: self.segment_tiers,
            query_reservation: self._query_reservation,
        }
    }
}

#[derive(Default)]
pub(super) struct InMemorySeriesSourceSnapshot {
    sealed_chunks: Vec<Arc<Chunk>>,
    active_points: ActiveSeriesSnapshot,
    query_reservation: Option<crate::QueryMemoryReservation>,
}

impl InMemorySeriesSourceSnapshot {
    #[cfg(test)]
    pub(super) fn active_point_count(&self) -> usize {
        self.active_points.point_count()
    }
}

pub(super) struct SeriesReadSnapshot {
    pub(super) persisted: PersistedSeriesSourceSnapshot,
    pub(super) sealed_chunks: Vec<Arc<Chunk>>,
    pub(super) active_points: ActiveSeriesSnapshot,
    pub(super) analysis: SeriesReadAnalysis,
    pub(super) query_reservation: Option<crate::QueryMemoryReservation>,
}

impl QuerySnapshotContext<'_> {
    fn snapshot_persisted_series_sources(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: Option<&QueryExecution>,
    ) -> Result<PersistedSeriesSourceSnapshot> {
        let persisted_index = self.persisted_index.read();

        let candidate_chunks = persisted_index
            .chunk_refs
            .get(&series_id)
            .map(|chunks| chunks.partition_point(|chunk| chunk.min_ts < end))
            .unwrap_or(0);
        let query_reservation = if let Some(execution) = execution {
            execution.checkpoint()?;
            execution
                .observe_intermediate_vector_size(saturating_u64_from_usize(candidate_chunks))?;
            Some(
                execution.reserve_memory(modeled_persisted_snapshot_build_upper_bound(
                    candidate_chunks,
                ))?,
            )
        } else {
            None
        };
        let mut snapshot = PersistedSeriesSourceSnapshot {
            chunks: Vec::with_capacity(candidate_chunks),
            segment_maps: HashMap::with_capacity(candidate_chunks),
            segment_tiers: HashMap::with_capacity(candidate_chunks),
            _query_reservation: query_reservation,
        };

        if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
            for chunk_ref in &chunks[..candidate_chunks] {
                if let Some(execution) = execution {
                    execution.checkpoint()?;
                }
                if chunk_ref.max_ts < start {
                    continue;
                }
                let chunk_tier = ChunkStorage::persisted_chunk_tier(&persisted_index, chunk_ref);
                if !ChunkStorage::plan_includes_persisted_tier(plan, chunk_tier) {
                    continue;
                }

                if let std::collections::hash_map::Entry::Vacant(entry) =
                    snapshot.segment_maps.entry(chunk_ref.segment_slot)
                {
                    let Some(segment_map) =
                        persisted_index.segment_maps.get(&chunk_ref.segment_slot)
                    else {
                        return Err(TsinkError::DataCorruption(format!(
                            "missing mapped segment slot {}",
                            chunk_ref.segment_slot
                        )));
                    };
                    entry.insert(Arc::clone(segment_map));
                    snapshot
                        .segment_tiers
                        .insert(chunk_ref.segment_slot, chunk_tier);
                }
                snapshot.chunks.push(*chunk_ref);
            }
        }

        Ok(snapshot)
    }

    fn snapshot_in_memory_series_sources_with_execution(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        execution: Option<&QueryExecution>,
    ) -> Result<(InMemorySeriesSourceSnapshot, MergePathShardSnapshotStats)> {
        let lock_wait_started = Instant::now();
        let active = self.chunks.active_shard(series_id).read();
        let sealed = self.chunks.sealed_shard(series_id).read();
        let lock_wait_nanos = elapsed_nanos_u64(lock_wait_started);
        let lock_hold_started = Instant::now();

        let mut active_points_visited = 0usize;
        let mut active_snapshot_bytes = 0u64;
        let mut active_snapshot_max_vector = 0usize;
        let mut nonempty_active_partitions = 0usize;
        if let Some(state) = active.get(&series_id) {
            let partition_window = self.partition_window.max(1);
            let start_partition = partition_id_for_timestamp(start, partition_window);
            let end_partition = partition_id_for_timestamp(end.saturating_sub(1), partition_window);
            if end > start {
                for (_, head) in state.partition_heads.range(start_partition..=end_partition) {
                    let model = head.builder.snapshot_in_range_memory_upper_bound(
                        start,
                        end,
                        QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES,
                    );
                    active_snapshot_bytes = active_snapshot_bytes.saturating_add(model.fixed_bytes);
                    active_snapshot_max_vector =
                        active_snapshot_max_vector.max(model.max_vector_len);
                    nonempty_active_partitions =
                        nonempty_active_partitions.saturating_add(usize::from(model.nonempty));
                    for point in head.builder.iter_points() {
                        active_points_visited = active_points_visited.saturating_add(1);
                        // Only partial blocks clone values, but charging every visited payload is
                        // a stable upper bound that also covers the transient zero-match build.
                        active_snapshot_bytes = active_snapshot_bytes
                            .saturating_add(modeled_value_retained_bytes(&point.value));
                    }
                }
            }
        }
        let active_partition_capacity =
            modeled_vec_growth_capacity_upper(nonempty_active_partitions);
        active_snapshot_bytes =
            active_snapshot_bytes.saturating_add(modeled_vec_capacity_bytes::<
                ActivePartitionSnapshot,
            >(active_partition_capacity));
        active_snapshot_max_vector = active_snapshot_max_vector.max(nonempty_active_partitions);

        let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
        let sealed_candidate_chunks = sealed
            .get(&series_id)
            .map(|chunks| chunks.range(..end_bound).count())
            .unwrap_or(0);
        let sealed_snapshot_capacity = modeled_vec_growth_capacity_upper(sealed_candidate_chunks);
        let sealed_snapshot_bytes =
            modeled_vec_capacity_bytes::<Arc<Chunk>>(sealed_snapshot_capacity);
        let query_reservation = if let Some(execution) = execution {
            execution.checkpoint()?;
            execution.observe_intermediate_vector_size(saturating_u64_from_usize(
                active_points_visited
                    .max(active_snapshot_max_vector)
                    .max(sealed_candidate_chunks),
            ))?;
            execution.charge_samples_scanned(saturating_u64_from_usize(active_points_visited))?;
            Some(
                execution
                    .reserve_memory(active_snapshot_bytes.saturating_add(sealed_snapshot_bytes))?,
            )
        } else {
            None
        };

        let mut snapshot = InMemorySeriesSourceSnapshot {
            sealed_chunks: Vec::with_capacity(sealed_snapshot_capacity),
            active_points: ActiveSeriesSnapshot::default(),
            query_reservation,
        };
        if let Some(chunks) = sealed.get(&series_id) {
            snapshot.sealed_chunks.extend(
                chunks
                    .range(..end_bound)
                    .filter(|(_, chunk)| chunk.header.max_ts >= start)
                    .map(|(_, chunk)| Arc::clone(chunk)),
            );
        }
        if let Some(state) = active.get(&series_id) {
            snapshot.active_points = state.snapshot_in_range(start, end, self.partition_window);
        }

        let lock_hold_nanos = elapsed_nanos_u64(lock_hold_started);
        drop(sealed);
        drop(active);

        Ok((
            snapshot,
            MergePathShardSnapshotStats::single(lock_wait_nanos, lock_hold_nanos),
        ))
    }

    pub(super) fn snapshot_series_read_sources(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: Option<&QueryExecution>,
    ) -> Result<(SeriesReadSnapshot, MergePathShardSnapshotStats)> {
        let visibility_guard = self.visibility_read_fence();
        let persisted =
            self.snapshot_persisted_series_sources(series_id, start, end, plan, execution)?;
        let (in_memory, shard_snapshot_stats) = self
            .snapshot_in_memory_series_sources_with_execution(series_id, start, end, execution)?;
        drop(visibility_guard);

        let InMemorySeriesSourceSnapshot {
            sealed_chunks,
            active_points,
            query_reservation,
        } = in_memory;
        let analysis = analyze_series_read_sources(
            &persisted.chunks,
            &sealed_chunks,
            &active_points,
            start,
            end,
        );

        Ok((
            SeriesReadSnapshot {
                persisted,
                sealed_chunks,
                active_points,
                analysis,
                query_reservation,
            },
            shard_snapshot_stats,
        ))
    }
}

impl ChunkStorage {
    fn persisted_chunk_tier(
        persisted_index: &PersistedIndexState,
        chunk_ref: &PersistedChunkRef,
    ) -> PersistedSegmentTier {
        persisted_index
            .segment_tiers
            .get(&chunk_ref.segment_slot)
            .copied()
            .unwrap_or(PersistedSegmentTier::Hot)
    }

    pub(in super::super) fn plan_includes_persisted_tier(
        plan: TieredQueryPlan,
        tier: PersistedSegmentTier,
    ) -> bool {
        match tier {
            PersistedSegmentTier::Hot => true,
            PersistedSegmentTier::Warm => plan.includes_warm(),
            PersistedSegmentTier::Cold => plan.includes_cold(),
        }
    }

    pub(super) fn record_merge_path_shard_snapshot_stats(
        &self,
        stats: MergePathShardSnapshotStats,
    ) {
        self.observability
            .query
            .merge_path_shard_snapshots_total
            .fetch_add(stats.snapshots, Ordering::Relaxed);
        self.observability
            .query
            .merge_path_shard_snapshot_wait_nanos_total
            .fetch_add(stats.wait_nanos, Ordering::Relaxed);
        self.observability
            .query
            .merge_path_shard_snapshot_hold_nanos_total
            .fetch_add(stats.hold_nanos, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn snapshot_in_memory_series_sources(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
    ) -> (InMemorySeriesSourceSnapshot, MergePathShardSnapshotStats) {
        self.query_snapshot_context()
            .snapshot_in_memory_series_sources_with_execution(series_id, start, end, None)
            .expect("unbudgeted in-memory snapshots cannot fail query admission")
    }

    #[cfg(test)]
    pub(super) fn snapshot_series_read_sources(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<(SeriesReadSnapshot, MergePathShardSnapshotStats)> {
        self.query_snapshot_context()
            .snapshot_series_read_sources(series_id, start, end, plan, None)
    }
}
