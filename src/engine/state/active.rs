use super::super::*;
#[cfg(test)]
use super::snapshot::record_active_series_snapshot_point_count;
use super::snapshot::{ActivePartitionSnapshot, ActiveSeriesSnapshot};

pub(in crate::engine::storage_engine) struct ActiveSeriesState {
    pub(in crate::engine::storage_engine) series_id: SeriesId,
    pub(in crate::engine::storage_engine) lane: ValueLane,
    pub(in crate::engine::storage_engine) point_cap: usize,
    pub(in crate::engine::storage_engine) partition_heads: BTreeMap<i64, ActivePartitionHead>,
    pub(in crate::engine::storage_engine) current_partition_id: Option<i64>,
}

pub(in crate::engine::storage_engine) struct ActivePartitionHead {
    pub(in crate::engine::storage_engine) builder: ChunkBuilder,
    pub(in crate::engine::storage_engine) builder_value_heap_bytes: usize,
    pub(in crate::engine::storage_engine) min_wal_highwater: Option<WalHighWatermark>,
    pub(in crate::engine::storage_engine) max_wal_highwater: WalHighWatermark,
}

pub(in crate::engine::storage_engine) enum PartitionHeadOpenAction {
    UseExisting,
    OpenNew { evict_partition_id: Option<i64> },
}

pub(in crate::engine::storage_engine) fn plan_partition_head_open<T>(
    partition_heads: &BTreeMap<i64, T>,
    next_partition: i64,
    ts: i64,
    max_partition_heads: usize,
) -> Result<PartitionHeadOpenAction> {
    if partition_heads.contains_key(&next_partition) {
        return Ok(PartitionHeadOpenAction::UseExisting);
    }

    let max_partition_heads = max_partition_heads.max(1);
    if partition_heads.len() < max_partition_heads {
        return Ok(PartitionHeadOpenAction::OpenNew {
            evict_partition_id: None,
        });
    }

    let Some(oldest_active_partition_id) = partition_heads.keys().next().copied() else {
        return Ok(PartitionHeadOpenAction::OpenNew {
            evict_partition_id: None,
        });
    };
    let newest_active_partition_id = partition_heads
        .keys()
        .next_back()
        .copied()
        .expect("non-empty partition head set must have a newest partition");

    // Once the fanout limit is full, keep a sliding window of the newest active
    // partitions. Existing heads remain writable, and mildly late partitions can
    // displace the oldest active head. Only writes older than the current active
    // window are rejected.
    if next_partition < oldest_active_partition_id {
        return Err(TsinkError::LateWritePartitionFanoutExceeded {
            timestamp: ts,
            partition_id: next_partition,
            max_active_partition_heads_per_series: max_partition_heads,
            oldest_active_partition_id,
            newest_active_partition_id,
        });
    }

    Ok(PartitionHeadOpenAction::OpenNew {
        evict_partition_id: Some(oldest_active_partition_id),
    })
}

impl ActivePartitionHead {
    fn new(series_id: SeriesId, lane: ValueLane, point_cap: usize) -> Self {
        Self {
            builder: ChunkBuilder::new(series_id, lane, point_cap),
            builder_value_heap_bytes: 0,
            min_wal_highwater: None,
            max_wal_highwater: WalHighWatermark::default(),
        }
    }

    fn snapshot_in_range(&self, start: i64, end: i64) -> ActivePartitionSnapshot {
        ActivePartitionSnapshot::from_points(self.builder.snapshot_in_range(start, end))
    }

    fn replace_builder(&mut self, series_id: SeriesId, lane: ValueLane, point_cap: usize) -> Self {
        let builder = std::mem::replace(
            &mut self.builder,
            ChunkBuilder::new(series_id, lane, point_cap),
        );
        let builder_value_heap_bytes = std::mem::take(&mut self.builder_value_heap_bytes);
        let min_wal_highwater = self.min_wal_highwater.take();
        let max_wal_highwater = std::mem::take(&mut self.max_wal_highwater);
        Self {
            builder,
            builder_value_heap_bytes,
            min_wal_highwater,
            max_wal_highwater,
        }
    }
}

impl ActiveSeriesState {
    pub(in crate::engine::storage_engine) fn new(
        series_id: SeriesId,
        lane: ValueLane,
        point_cap: usize,
    ) -> Self {
        Self {
            series_id,
            lane,
            point_cap,
            partition_heads: BTreeMap::new(),
            current_partition_id: None,
        }
    }

    pub(in crate::engine::storage_engine) fn append_point(
        &mut self,
        ts: i64,
        value: Value,
        wal_highwater: WalHighWatermark,
    ) {
        let partition_id = self
            .current_partition_id
            .expect("rotate_partition_if_needed must run before append_point");
        let head = self
            .partition_heads
            .get_mut(&partition_id)
            .expect("active partition head must exist before append_point");
        head.builder_value_heap_bytes = head
            .builder_value_heap_bytes
            .saturating_add(super::super::value_heap_bytes(&value));
        head.builder.append(ts, value);
        head.min_wal_highwater = Some(
            head.min_wal_highwater
                .map_or(wal_highwater, |current| current.min(wal_highwater)),
        );
        head.max_wal_highwater = head.max_wal_highwater.max(wal_highwater);
    }

    pub(in crate::engine::storage_engine) fn rotate_partition_if_needed(
        &mut self,
        ts: i64,
        partition_window: i64,
        max_partition_heads: usize,
    ) -> Result<Option<Chunk>> {
        let partition_window = partition_window.max(1);
        let next_partition = super::super::partition_id_for_timestamp(ts, partition_window);
        let mut evicted = None;
        match plan_partition_head_open(
            &self.partition_heads,
            next_partition,
            ts,
            max_partition_heads,
        )? {
            PartitionHeadOpenAction::UseExisting => {
                self.current_partition_id = Some(next_partition);
                return Ok(None);
            }
            PartitionHeadOpenAction::OpenNew { evict_partition_id } => {
                if let Some(partition_id) = evict_partition_id {
                    evicted = self.finalize_partition_head(partition_id)?;
                }
            }
        }

        self.current_partition_id = Some(next_partition);
        self.partition_heads
            .entry(next_partition)
            .or_insert_with(|| ActivePartitionHead::new(self.series_id, self.lane, self.point_cap));
        Ok(evicted)
    }

    pub(in crate::engine::storage_engine) fn rotate_full_if_needed(
        &mut self,
    ) -> Result<Option<Chunk>> {
        let Some(partition_id) = self.current_partition_id else {
            return Ok(None);
        };
        if !self
            .partition_heads
            .get(&partition_id)
            .is_some_and(|head| head.builder.is_full())
        {
            return Ok(None);
        }
        self.finalize_full_partition_chunk(partition_id)
    }

    pub(in crate::engine::storage_engine) fn flush_partial(&mut self) -> Result<Option<Chunk>> {
        let Some(partition_id) = self.partition_heads.keys().next().copied() else {
            return Ok(None);
        };
        self.finalize_partition_head(partition_id)
    }

    pub(in crate::engine::storage_engine) fn flush_background_eligible_partial(
        &mut self,
    ) -> Result<Option<Chunk>> {
        // Background-eligible flushes keep the current head open so callers like admission
        // pressure relief can persist older work without fragmenting the live head.
        let Some(partition_id) = self
            .partition_heads
            .keys()
            .copied()
            .find(|partition_id| Some(*partition_id) != self.current_partition_id)
        else {
            return Ok(None);
        };
        self.finalize_partition_head(partition_id)
    }

    pub(in crate::engine::storage_engine) fn flush_current_partial(
        &mut self,
    ) -> Result<Option<Chunk>> {
        let Some(partition_id) = self.current_partition_id else {
            return Ok(None);
        };
        let Some(head) = self.partition_heads.get(&partition_id) else {
            return Ok(None);
        };
        if head.builder.is_empty() {
            return Ok(None);
        }
        self.finalize_partition_head(partition_id)
    }

    pub(in crate::engine::storage_engine) fn is_empty(&self) -> bool {
        self.partition_heads.is_empty()
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn point_count(&self) -> usize {
        self.partition_heads
            .values()
            .map(|head| head.builder.len())
            .sum()
    }

    pub(in crate::engine::storage_engine) fn partition_head_count(&self) -> usize {
        self.partition_heads.len()
    }

    pub(in crate::engine::storage_engine) fn contains_partition_head(
        &self,
        partition_id: i64,
    ) -> bool {
        self.partition_heads.contains_key(&partition_id)
    }

    pub(in crate::engine::storage_engine) fn snapshot_in_range(
        &self,
        start: i64,
        end: i64,
        partition_window: i64,
    ) -> ActiveSeriesSnapshot {
        if end <= start {
            return ActiveSeriesSnapshot::default();
        }

        let partition_window = partition_window.max(1);
        let start_partition = super::super::partition_id_for_timestamp(start, partition_window);
        let end_partition =
            super::super::partition_id_for_timestamp(end.saturating_sub(1), partition_window);

        let snapshot = ActiveSeriesSnapshot::from_partition_heads(
            self.partition_heads
                .range(start_partition..=end_partition)
                .filter_map(|(_, head)| {
                    let snapshot = head.snapshot_in_range(start, end);
                    (!snapshot.is_empty()).then_some(snapshot)
                })
                .collect(),
        );

        #[cfg(test)]
        record_active_series_snapshot_point_count(snapshot.point_count());

        snapshot
    }

    pub(in crate::engine::storage_engine) fn points_in_partition_order(
        &self,
    ) -> impl Iterator<Item = &ChunkPoint> + '_ {
        self.partition_heads
            .values()
            .flat_map(|head| head.builder.iter_points())
    }

    pub(in crate::engine::storage_engine) fn min_wal_highwater(&self) -> Option<WalHighWatermark> {
        self.partition_heads
            .values()
            .filter_map(|head| head.min_wal_highwater)
            .min()
    }

    fn finalize_partition_head(&mut self, partition_id: i64) -> Result<Option<Chunk>> {
        let Some(head) = self.partition_heads.remove(&partition_id) else {
            return Ok(None);
        };
        if self.current_partition_id == Some(partition_id) {
            self.current_partition_id = self.partition_heads.keys().next_back().copied();
        }

        self.finalize_head_chunk(head)
    }

    fn finalize_full_partition_chunk(&mut self, partition_id: i64) -> Result<Option<Chunk>> {
        let Some(head) = self.partition_heads.get_mut(&partition_id) else {
            return Ok(None);
        };
        let sealed_head = head.replace_builder(self.series_id, self.lane, self.point_cap);
        self.finalize_head_chunk(sealed_head)
    }

    fn finalize_head_chunk(&self, head: ActivePartitionHead) -> Result<Option<Chunk>> {
        if head.builder.is_empty() {
            return Ok(None);
        }

        let points_are_sorted = head.builder.is_sorted_by_ts();
        let mut chunk = head
            .builder
            .finalize(
                chunk::TimestampCodecId::DeltaVarint,
                chunk::ValueCodecId::ConstantRle,
            )
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration("failed to finalize chunk".to_string())
            })?;

        if !points_are_sorted {
            // Preserve a monotonic timestamp stream per chunk for better timestamp codec density.
            chunk.points.sort_by_key(|point| point.ts);
        }

        let encoded = Encoder::encode_chunk_points(&chunk.points, self.lane)?;
        chunk.header.ts_codec = encoded.ts_codec;
        chunk.header.value_codec = encoded.value_codec;
        chunk.header.value_family = Some(Encoder::infer_series_value_family(
            &chunk.points,
            self.lane,
        )?);
        chunk.encoded_payload = encoded.payload;
        chunk.wal_highwater = head.max_wal_highwater;

        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn active_series_snapshot_in_range_skips_irrelevant_partitions() {
        let mut state = ActiveSeriesState::new(1, ValueLane::Numeric, 8);
        for ts in [1, 2, 12, 17, 22] {
            state.rotate_partition_if_needed(ts, 10, 8).unwrap();
            state.append_point(ts, Value::I64(ts), WalHighWatermark::default());
        }

        let points = state
            .snapshot_in_range(10, 20, 10)
            .into_points_in_partition_order()
            .into_iter()
            .map(|point| point.ts)
            .collect::<Vec<_>>();
        assert_eq!(points, vec![12, 17]);
    }
}
