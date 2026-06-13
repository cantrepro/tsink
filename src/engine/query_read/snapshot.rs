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
}

impl PersistedSeriesSourceSnapshot {
    pub(super) fn chunk_tier(&self, chunk_ref: &PersistedChunkRef) -> PersistedSegmentTier {
        self.segment_tiers
            .get(&chunk_ref.segment_slot)
            .copied()
            .unwrap_or(PersistedSegmentTier::Hot)
    }
}

#[derive(Default)]
pub(super) struct InMemorySeriesSourceSnapshot {
    sealed_chunks: Vec<Arc<Chunk>>,
    active_points: ActiveSeriesSnapshot,
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
}

impl QuerySnapshotContext<'_> {
    fn snapshot_persisted_series_sources(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<PersistedSeriesSourceSnapshot> {
        let mut snapshot = PersistedSeriesSourceSnapshot::default();
        let mut segment_slots = BTreeSet::<usize>::new();
        let persisted_index = self.persisted_index.read();

        if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
            let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
            snapshot.chunks.reserve(end_idx);
            for chunk_ref in &chunks[..end_idx] {
                if chunk_ref.max_ts < start {
                    continue;
                }
                let chunk_tier = ChunkStorage::persisted_chunk_tier(&persisted_index, chunk_ref);
                if !ChunkStorage::plan_includes_persisted_tier(plan, chunk_tier) {
                    continue;
                }

                segment_slots.insert(chunk_ref.segment_slot);
                snapshot.chunks.push(*chunk_ref);
            }
        }

        snapshot.segment_maps.reserve(segment_slots.len());
        snapshot.segment_tiers.reserve(segment_slots.len());
        for slot in &segment_slots {
            let Some(segment_map) = persisted_index.segment_maps.get(slot) else {
                return Err(TsinkError::DataCorruption(format!(
                    "missing mapped segment slot {}",
                    slot
                )));
            };
            snapshot.segment_maps.insert(*slot, Arc::clone(segment_map));
            snapshot.segment_tiers.insert(
                *slot,
                persisted_index
                    .segment_tiers
                    .get(slot)
                    .copied()
                    .unwrap_or(PersistedSegmentTier::Hot),
            );
        }

        Ok(snapshot)
    }

    pub(super) fn snapshot_in_memory_series_sources(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
    ) -> (InMemorySeriesSourceSnapshot, MergePathShardSnapshotStats) {
        let lock_wait_started = Instant::now();
        let active = self.chunks.active_shard(series_id).read();
        let sealed = self.chunks.sealed_shard(series_id).read();
        let lock_wait_nanos = elapsed_nanos_u64(lock_wait_started);
        let lock_hold_started = Instant::now();

        let mut snapshot = InMemorySeriesSourceSnapshot::default();
        if let Some(chunks) = sealed.get(&series_id) {
            let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
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

        (
            snapshot,
            MergePathShardSnapshotStats::single(lock_wait_nanos, lock_hold_nanos),
        )
    }

    pub(super) fn snapshot_series_read_sources(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<(SeriesReadSnapshot, MergePathShardSnapshotStats)> {
        let visibility_guard = self.visibility_read_fence();
        let persisted = self.snapshot_persisted_series_sources(series_id, start, end, plan)?;
        let (in_memory, shard_snapshot_stats) =
            self.snapshot_in_memory_series_sources(series_id, start, end);
        drop(visibility_guard);

        let InMemorySeriesSourceSnapshot {
            sealed_chunks,
            active_points,
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
            .snapshot_in_memory_series_sources(series_id, start, end)
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
            .snapshot_series_read_sources(series_id, start, end, plan)
    }
}
