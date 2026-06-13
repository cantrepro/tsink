use super::*;

pub(super) struct ActiveSeriesState {
    pub(super) series_id: SeriesId,
    pub(super) lane: ValueLane,
    pub(super) point_cap: usize,
    pub(super) builder: ChunkBuilder,
    pub(super) builder_value_heap_bytes: usize,
    pub(super) partition_id: Option<i64>,
}

impl ActiveSeriesState {
    pub(super) fn new(series_id: SeriesId, lane: ValueLane, point_cap: usize) -> Self {
        Self {
            series_id,
            lane,
            point_cap,
            builder: ChunkBuilder::new(series_id, lane, point_cap),
            builder_value_heap_bytes: 0,
            partition_id: None,
        }
    }

    pub(super) fn append_point(&mut self, ts: i64, value: Value) {
        self.builder_value_heap_bytes = self
            .builder_value_heap_bytes
            .saturating_add(super::value_heap_bytes(&value));
        self.builder.append(ts, value);
    }

    pub(super) fn rotate_partition_if_needed(
        &mut self,
        ts: i64,
        partition_window: i64,
    ) -> Result<Option<Chunk>> {
        let partition_window = partition_window.max(1);
        let next_partition = super::partition_id_for_timestamp(ts, partition_window);

        if self.builder.is_empty() {
            self.partition_id = Some(next_partition);
            return Ok(None);
        }

        if self.partition_id.is_none() {
            self.partition_id = self
                .builder
                .first_point()
                .map(|first| super::partition_id_for_timestamp(first.ts, partition_window));
        }

        if self.partition_id == Some(next_partition) {
            return Ok(None);
        }

        let chunk = self.finalize_current()?;
        self.partition_id = Some(next_partition);
        Ok(chunk)
    }

    pub(super) fn rotate_full_if_needed(&mut self) -> Result<Option<Chunk>> {
        if !self.builder.is_full() {
            return Ok(None);
        }
        let chunk = self.finalize_current()?;
        self.partition_id = None;
        Ok(chunk)
    }

    pub(super) fn flush_partial(&mut self) -> Result<Option<Chunk>> {
        if self.builder.is_empty() {
            return Ok(None);
        }
        let chunk = self.finalize_current()?;
        self.partition_id = None;
        Ok(chunk)
    }

    fn finalize_current(&mut self) -> Result<Option<Chunk>> {
        let old_builder = std::mem::replace(
            &mut self.builder,
            ChunkBuilder::new(self.series_id, self.lane, self.point_cap),
        );
        let points_are_sorted = old_builder.is_sorted_by_ts();
        self.builder_value_heap_bytes = 0;
        let mut chunk = old_builder
            .finalize(
                super::chunk::TimestampCodecId::DeltaVarint,
                super::chunk::ValueCodecId::ConstantRle,
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
        chunk.encoded_payload = encoded.payload;

        Ok(Some(chunk))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SealedChunkKey {
    pub(super) min_ts: i64,
    pub(super) max_ts: i64,
    pub(super) point_count: u16,
    pub(super) sequence: u64,
}

impl SealedChunkKey {
    pub(super) fn from_chunk(chunk: &Chunk, sequence: u64) -> Self {
        Self {
            min_ts: chunk.header.min_ts,
            max_ts: chunk.header.max_ts,
            point_count: chunk.header.point_count,
            sequence,
        }
    }

    pub(super) fn upper_bound_for_min_ts(min_ts_exclusive: i64) -> Self {
        Self {
            min_ts: min_ts_exclusive,
            max_ts: i64::MIN,
            point_count: 0,
            sequence: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PersistedChunkRef {
    pub(super) level: u8,
    pub(super) min_ts: i64,
    pub(super) max_ts: i64,
    pub(super) point_count: u16,
    pub(super) sequence: u64,
    pub(super) chunk_offset: u64,
    pub(super) chunk_len: u32,
    pub(super) lane: ValueLane,
    pub(super) ts_codec: super::chunk::TimestampCodecId,
    pub(super) value_codec: super::chunk::ValueCodecId,
    pub(super) segment_slot: usize,
}

#[derive(Default)]
pub(super) struct PersistedIndexState {
    pub(super) chunk_refs: HashMap<SeriesId, Vec<PersistedChunkRef>>,
    pub(super) segment_maps: Vec<Arc<PlatformMmap>>,
}

pub(super) const NUMERIC_LANE_ROOT: &str = "lane_numeric";
pub(super) const BLOB_LANE_ROOT: &str = "lane_blob";
pub(super) const WAL_DIR_NAME: &str = "wal";
pub(super) const SERIES_INDEX_FILE_NAME: &str = "series_index.bin";
