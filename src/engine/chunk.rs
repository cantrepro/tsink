use crate::value::Value;

use super::series_registry::SeriesId;

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

#[derive(Debug, Clone)]
pub struct Chunk {
    pub header: ChunkHeader,
    pub points: Vec<ChunkPoint>,
    pub encoded_payload: Vec<u8>,
}

#[derive(Debug)]
pub struct ChunkBuilder {
    series_id: SeriesId,
    lane: ValueLane,
    max_points: usize,
    points: Vec<ChunkPoint>,
}

impl ChunkBuilder {
    pub fn new(series_id: SeriesId, lane: ValueLane, max_points: usize) -> Self {
        Self {
            series_id,
            lane,
            max_points: max_points.max(1),
            points: Vec::with_capacity(max_points.max(1)),
        }
    }

    pub fn append(&mut self, ts: i64, value: Value) {
        self.points.push(ChunkPoint { ts, value });
    }

    pub fn is_full(&self) -> bool {
        self.points.len() >= self.max_points
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn capacity(&self) -> usize {
        self.points.capacity()
    }

    pub fn first_point(&self) -> Option<&ChunkPoint> {
        self.points.first()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn points(&self) -> &[ChunkPoint] {
        &self.points
    }

    pub fn finalize(self, ts_codec: TimestampCodecId, value_codec: ValueCodecId) -> Option<Chunk> {
        if self.points.is_empty() {
            return None;
        }

        let min_ts = self.points.iter().map(|p| p.ts).min()?;
        let max_ts = self.points.iter().map(|p| p.ts).max()?;
        let point_count = u16::try_from(self.points.len()).ok()?;

        Some(Chunk {
            header: ChunkHeader {
                series_id: self.series_id,
                lane: self.lane,
                point_count,
                min_ts,
                max_ts,
                ts_codec,
                value_codec,
            },
            points: self.points,
            encoded_payload: Vec::new(),
        })
    }
}
