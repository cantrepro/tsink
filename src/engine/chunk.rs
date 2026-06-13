use std::sync::Arc;

use crate::{value::Value, Result};

use crate::engine::durability::WalHighWatermark;

use super::encoder::Encoder;
use super::series::SeriesId;
use super::series::SeriesValueFamily;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueLane {
    Numeric = 0,
    Blob = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimestampCodecId {
    FixedStepRle = 1,
    DeltaOfDeltaBitpack = 2,
    DeltaVarint = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueCodecId {
    GorillaXorF64 = 1,
    ZigZagDeltaBitpackI64 = 2,
    DeltaBitpackU64 = 3,
    ConstantRle = 4,
    BoolBitpack = 5,
    BytesDeltaBlock = 6,
}

#[derive(Debug, Clone)]
pub struct ChunkHeader {
    pub series_id: SeriesId,
    pub lane: ValueLane,
    pub value_family: Option<SeriesValueFamily>,
    pub point_count: u16,
    pub min_ts: i64,
    pub max_ts: i64,
    pub ts_codec: TimestampCodecId,
    pub value_codec: ValueCodecId,
}

#[derive(Debug, Clone)]
pub struct ChunkPoint {
    pub ts: i64,
    pub value: Value,
}

const ACTIVE_POINT_SNAPSHOT_BLOCK_POINTS: usize = 64;

#[derive(Debug)]
pub struct Chunk {
    pub header: ChunkHeader,
    pub points: Vec<ChunkPoint>,
    pub encoded_payload: Vec<u8>,
    pub wal_highwater: WalHighWatermark,
}

impl Clone for Chunk {
    fn clone(&self) -> Self {
        #[cfg(test)]
        CHUNK_CLONE_COUNT.with(|count| count.set(count.get().saturating_add(1)));

        Self {
            header: self.header.clone(),
            points: self.points.clone(),
            encoded_payload: self.encoded_payload.clone(),
            wal_highwater: self.wal_highwater,
        }
    }
}

impl AsRef<Chunk> for Chunk {
    fn as_ref(&self) -> &Chunk {
        self
    }
}

#[cfg(test)]
std::thread_local! {
    static CHUNK_CLONE_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_chunk_clone_count() {
    CHUNK_CLONE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn chunk_clone_count() -> usize {
    CHUNK_CLONE_COUNT.with(std::cell::Cell::get)
}

impl Chunk {
    pub(crate) fn into_sealed_storage(mut self) -> Self {
        // Keep only the encoded representation for finalized sealed chunks by default.
        if !self.encoded_payload.is_empty() {
            self.points = Vec::new();
        }
        self
    }

    pub(crate) fn decode_points(&self) -> Result<Vec<ChunkPoint>> {
        if !self.points.is_empty() {
            return Ok(self.points.clone());
        }
        if self.encoded_payload.is_empty() {
            return Ok(Vec::new());
        }

        Encoder::decode_chunk_points_from_payload(
            self.header.lane,
            self.header.ts_codec,
            self.header.value_codec,
            self.header.point_count as usize,
            &self.encoded_payload,
        )
    }
}

#[derive(Debug)]
pub struct ChunkBuilder {
    series_id: SeriesId,
    lane: ValueLane,
    max_points: usize,
    point_block_max_points: usize,
    frozen_point_blocks: Vec<FrozenPointBlock>,
    tail_points: Vec<ChunkPoint>,
    point_count: usize,
    is_sorted_by_ts: bool,
    last_ts: Option<i64>,
}

#[derive(Debug, Clone)]
struct FrozenPointBlock {
    points: Arc<Vec<ChunkPoint>>,
    min_ts: i64,
    max_ts: i64,
}

impl FrozenPointBlock {
    fn new(points: Vec<ChunkPoint>) -> Self {
        debug_assert!(!points.is_empty());

        let min_ts = points
            .iter()
            .map(|point| point.ts)
            .min()
            .unwrap_or(i64::MAX);
        let max_ts = points
            .iter()
            .map(|point| point.ts)
            .max()
            .unwrap_or(i64::MIN);

        Self {
            points: Arc::new(points),
            min_ts,
            max_ts,
        }
    }

    fn capacity(&self) -> usize {
        self.points.capacity()
    }

    fn first_point(&self) -> Option<&ChunkPoint> {
        self.points.first()
    }

    fn overlaps_range(&self, start: i64, end: i64) -> bool {
        self.max_ts >= start && self.min_ts < end
    }

    fn fully_within_range(&self, start: i64, end: i64) -> bool {
        self.min_ts >= start && self.max_ts < end
    }

    fn into_points(self) -> Vec<ChunkPoint> {
        match Arc::try_unwrap(self.points) {
            Ok(points) => points,
            Err(points) => points.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone)]
enum ChunkBuilderSnapshotBlock {
    Shared(Arc<Vec<ChunkPoint>>),
    Owned(Vec<ChunkPoint>),
}

impl ChunkBuilderSnapshotBlock {
    fn as_slice(&self) -> &[ChunkPoint] {
        match self {
            Self::Shared(points) => points.as_slice(),
            Self::Owned(points) => points.as_slice(),
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn get(&self, idx: usize) -> Option<&ChunkPoint> {
        self.as_slice().get(idx)
    }

    #[cfg(test)]
    fn into_points(self) -> Vec<ChunkPoint> {
        match self {
            Self::Shared(points) => match Arc::try_unwrap(points) {
                Ok(points) => points,
                Err(points) => points.iter().cloned().collect(),
            },
            Self::Owned(points) => points,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChunkBuilderSnapshot {
    point_blocks: Vec<ChunkBuilderSnapshotBlock>,
    #[cfg(test)]
    #[allow(dead_code)]
    point_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkBuilderSnapshotCursor {
    snapshot: ChunkBuilderSnapshot,
    next_block_idx: usize,
    next_point_idx_in_block: usize,
}

impl ChunkBuilderSnapshot {
    fn push_block(&mut self, block: ChunkBuilderSnapshotBlock) {
        let point_count = block.len();
        if point_count == 0 {
            return;
        }

        #[cfg(test)]
        {
            self.point_count = self.point_count.saturating_add(point_count);
        }

        self.point_blocks.push(block);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.point_blocks.is_empty()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.point_count
    }

    pub(crate) fn iter_points(&self) -> impl Iterator<Item = &ChunkPoint> + '_ {
        self.point_blocks
            .iter()
            .flat_map(|block| block.as_slice().iter())
    }

    pub(crate) fn into_cursor(self) -> ChunkBuilderSnapshotCursor {
        ChunkBuilderSnapshotCursor {
            snapshot: self,
            next_block_idx: 0,
            next_point_idx_in_block: 0,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn into_points(self) -> Vec<ChunkPoint> {
        let point_count = self
            .point_blocks
            .iter()
            .map(ChunkBuilderSnapshotBlock::len)
            .sum();
        let mut points = Vec::with_capacity(point_count);
        for block in self.point_blocks {
            points.extend(block.into_points());
        }
        points
    }
}

impl ChunkBuilderSnapshotCursor {
    pub(crate) fn peek(&self) -> Option<&ChunkPoint> {
        self.snapshot
            .point_blocks
            .get(self.next_block_idx)
            .and_then(|block| block.get(self.next_point_idx_in_block))
    }

    pub(crate) fn advance(&mut self) -> bool {
        if let Some(block) = self.snapshot.point_blocks.get(self.next_block_idx) {
            if self.next_point_idx_in_block + 1 < block.len() {
                self.next_point_idx_in_block = self.next_point_idx_in_block.saturating_add(1);
            } else {
                self.next_block_idx = self.next_block_idx.saturating_add(1);
                self.next_point_idx_in_block = 0;
            }
            return true;
        }

        false
    }
}

impl ChunkBuilder {
    pub fn new(series_id: SeriesId, lane: ValueLane, max_points: usize) -> Self {
        let max_points = max_points.max(1);
        let point_block_max_points = max_points.min(ACTIVE_POINT_SNAPSHOT_BLOCK_POINTS);
        Self {
            series_id,
            lane,
            max_points,
            point_block_max_points,
            frozen_point_blocks: Vec::new(),
            tail_points: Vec::with_capacity(point_block_max_points),
            point_count: 0,
            is_sorted_by_ts: true,
            last_ts: None,
        }
    }

    pub fn append(&mut self, ts: i64, value: Value) {
        if let Some(last_ts) = self.last_ts {
            if ts < last_ts {
                self.is_sorted_by_ts = false;
            }
        }
        self.last_ts = Some(ts);
        self.tail_points.push(ChunkPoint { ts, value });
        self.point_count = self.point_count.saturating_add(1);
        self.freeze_full_tail_if_needed();
    }

    pub fn is_full(&self) -> bool {
        self.point_count >= self.max_points
    }

    pub fn len(&self) -> usize {
        self.point_count
    }

    pub fn capacity(&self) -> usize {
        self.frozen_point_blocks
            .iter()
            .fold(self.tail_points.capacity(), |capacity, block| {
                capacity.saturating_add(block.capacity())
            })
    }

    pub(crate) fn point_block_capacity(&self) -> usize {
        self.frozen_point_blocks.capacity()
    }

    pub(crate) fn frozen_point_block_count(&self) -> usize {
        self.frozen_point_blocks.len()
    }

    pub fn first_point(&self) -> Option<&ChunkPoint> {
        self.frozen_point_blocks
            .first()
            .and_then(FrozenPointBlock::first_point)
            .or_else(|| self.tail_points.first())
    }

    pub fn is_empty(&self) -> bool {
        self.point_count == 0
    }

    pub(crate) fn iter_points(&self) -> impl Iterator<Item = &ChunkPoint> + '_ {
        self.frozen_point_blocks
            .iter()
            .flat_map(|block| block.points.iter())
            .chain(self.tail_points.iter())
    }

    pub(crate) fn is_sorted_by_ts(&self) -> bool {
        self.is_sorted_by_ts
    }

    pub(crate) fn snapshot_in_range(&self, start: i64, end: i64) -> ChunkBuilderSnapshot {
        if end <= start {
            return ChunkBuilderSnapshot::default();
        }

        let mut snapshot = ChunkBuilderSnapshot::default();
        snapshot
            .point_blocks
            .reserve(self.frozen_point_blocks.len() + usize::from(!self.tail_points.is_empty()));

        for block in &self.frozen_point_blocks {
            if !block.overlaps_range(start, end) {
                continue;
            }

            if block.fully_within_range(start, end) {
                snapshot.push_block(ChunkBuilderSnapshotBlock::Shared(Arc::clone(&block.points)));
                continue;
            }

            let filtered_points = block
                .points
                .iter()
                .filter(|point| point.ts >= start && point.ts < end)
                .cloned()
                .collect();
            snapshot.push_block(ChunkBuilderSnapshotBlock::Owned(filtered_points));
        }

        let tail_points = self
            .tail_points
            .iter()
            .filter(|point| point.ts >= start && point.ts < end)
            .cloned()
            .collect();
        snapshot.push_block(ChunkBuilderSnapshotBlock::Owned(tail_points));
        snapshot
    }

    pub fn finalize(self, ts_codec: TimestampCodecId, value_codec: ValueCodecId) -> Option<Chunk> {
        if self.point_count == 0 {
            return None;
        }

        let series_id = self.series_id;
        let lane = self.lane;
        let points = self.into_points();
        let min_ts = points.iter().map(|p| p.ts).min()?;
        let max_ts = points.iter().map(|p| p.ts).max()?;
        let point_count = u16::try_from(points.len()).ok()?;

        Some(Chunk {
            header: ChunkHeader {
                series_id,
                lane,
                value_family: None,
                point_count,
                min_ts,
                max_ts,
                ts_codec,
                value_codec,
            },
            points,
            encoded_payload: Vec::new(),
            wal_highwater: WalHighWatermark::default(),
        })
    }

    fn freeze_full_tail_if_needed(&mut self) {
        if self.tail_points.len() < self.point_block_max_points {
            return;
        }

        let mut frozen = Vec::with_capacity(self.point_block_max_points);
        std::mem::swap(&mut frozen, &mut self.tail_points);
        self.frozen_point_blocks.push(FrozenPointBlock::new(frozen));
        self.tail_points = Vec::with_capacity(self.point_block_max_points);
    }

    fn into_points(self) -> Vec<ChunkPoint> {
        let mut points = Vec::with_capacity(self.point_count);
        for block in self.frozen_point_blocks {
            points.extend(block.into_points());
        }
        points.extend(self.tail_points);
        points
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkBuilder, ValueLane};
    use crate::value::Value;

    #[test]
    fn builder_tracks_monotonic_timestamps() {
        let mut builder = ChunkBuilder::new(1, ValueLane::Numeric, 4);
        builder.append(10, Value::I64(1));
        builder.append(11, Value::I64(2));
        builder.append(11, Value::I64(3));
        builder.append(15, Value::I64(4));
        assert!(builder.is_sorted_by_ts());
    }

    #[test]
    fn builder_marks_unsorted_after_backwards_append() {
        let mut builder = ChunkBuilder::new(1, ValueLane::Numeric, 4);
        builder.append(10, Value::I64(1));
        builder.append(12, Value::I64(2));
        builder.append(11, Value::I64(3));
        assert!(!builder.is_sorted_by_ts());
    }

    #[test]
    fn snapshot_in_range_preserves_block_order_and_skips_irrelevant_points() {
        let mut builder = ChunkBuilder::new(1, ValueLane::Numeric, 4);
        builder.append(1, Value::I64(1));
        builder.append(2, Value::I64(2));
        builder.append(3, Value::I64(3));
        builder.append(4, Value::I64(4));
        builder.append(10, Value::I64(10));
        builder.append(7, Value::I64(7));
        builder.append(8, Value::I64(8));
        builder.append(9, Value::I64(9));
        builder.append(12, Value::I64(12));
        builder.append(11, Value::I64(11));

        let points = builder
            .snapshot_in_range(3, 11)
            .into_points()
            .into_iter()
            .map(|point| point.ts)
            .collect::<Vec<_>>();
        assert_eq!(points, vec![3, 4, 10, 7, 8, 9]);
    }
}
