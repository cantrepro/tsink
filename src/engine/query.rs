use crate::{DataPoint, Label, Result};

use super::chunk::Chunk;
use super::encoder::{EncodedChunk, Encoder};
use super::series_registry::SeriesId;

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub metric: String,
    pub labels: Vec<Label>,
    pub start: i64,
    pub end: i64,
    pub candidate_series: Vec<SeriesId>,
}

impl QueryPlan {
    pub fn new(metric: impl Into<String>, labels: Vec<Label>, start: i64, end: i64) -> Self {
        Self {
            metric: metric.into(),
            labels,
            start,
            end,
            candidate_series: Vec::new(),
        }
    }
}

pub struct ChunkSeriesCursor<'a> {
    chunks: &'a [Chunk],
    pos: usize,
    end: usize,
}

impl<'a> ChunkSeriesCursor<'a> {
    pub fn new(chunks: &'a [Chunk], start: i64, end: i64) -> Self {
        if chunks.is_empty() || start >= end {
            return Self {
                chunks,
                pos: 0,
                end: 0,
            };
        }

        // First chunk where max_ts >= start.
        let first = chunks.partition_point(|chunk| chunk.header.max_ts < start);
        // First chunk where min_ts >= end (exclusive bound).
        let end_idx = chunks.partition_point(|chunk| chunk.header.min_ts < end);

        Self {
            chunks,
            pos: first.min(end_idx),
            end: end_idx,
        }
    }
}

impl<'a> Iterator for ChunkSeriesCursor<'a> {
    type Item = &'a Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.end {
            return None;
        }

        let out = self.chunks.get(self.pos);
        self.pos = self.pos.saturating_add(1);
        out
    }
}

pub fn decode_chunk_points_in_range_into(
    chunk: &Chunk,
    start: i64,
    end: i64,
    out: &mut Vec<DataPoint>,
) -> Result<()> {
    if chunk.points.is_empty() && !chunk.encoded_payload.is_empty() {
        let decoded = Encoder::decode_chunk_points(&EncodedChunk {
            lane: chunk.header.lane,
            ts_codec: chunk.header.ts_codec,
            value_codec: chunk.header.value_codec,
            point_count: chunk.header.point_count as usize,
            payload: chunk.encoded_payload.clone(),
        })?;
        append_sorted_owned_chunk_points_in_range(decoded, start, end, out);
        return Ok(());
    }

    // Canonical in-memory chunks are finalized from sorted points and always carry encoded payload.
    if !chunk.encoded_payload.is_empty() {
        debug_assert!(points_are_sorted_by_timestamp(&chunk.points));
        append_sorted_chunk_points_in_range(&chunk.points, start, end, out);
        return Ok(());
    }

    if points_are_sorted_by_timestamp(&chunk.points) {
        append_sorted_chunk_points_in_range(&chunk.points, start, end, out);
        return Ok(());
    }

    for point in &chunk.points {
        if point.ts >= start && point.ts < end {
            out.push(DataPoint::new(point.ts, point.value.clone()));
        }
    }

    Ok(())
}

pub fn decode_chunk_points_in_range(chunk: &Chunk, start: i64, end: i64) -> Result<Vec<DataPoint>> {
    let mut out = Vec::new();
    decode_chunk_points_in_range_into(chunk, start, end, &mut out)?;
    Ok(out)
}

fn append_sorted_chunk_points_in_range(
    points: &[super::chunk::ChunkPoint],
    start: i64,
    end: i64,
    out: &mut Vec<DataPoint>,
) {
    let first = points.partition_point(|point| point.ts < start);
    let last = points.partition_point(|point| point.ts < end);
    out.reserve(last.saturating_sub(first));
    for point in &points[first..last] {
        out.push(DataPoint {
            timestamp: point.ts,
            value: point.value.clone(),
        });
    }
}

fn append_sorted_owned_chunk_points_in_range(
    points: Vec<super::chunk::ChunkPoint>,
    start: i64,
    end: i64,
    out: &mut Vec<DataPoint>,
) {
    let first = points.partition_point(|point| point.ts < start);
    let last = points.partition_point(|point| point.ts < end);
    out.reserve(last.saturating_sub(first));
    for point in points
        .into_iter()
        .skip(first)
        .take(last.saturating_sub(first))
    {
        out.push(DataPoint {
            timestamp: point.ts,
            value: point.value,
        });
    }
}

fn points_are_sorted_by_timestamp(points: &[super::chunk::ChunkPoint]) -> bool {
    points.windows(2).all(|pair| pair[0].ts <= pair[1].ts)
}

#[cfg(test)]
mod tests {
    use crate::Value;

    use super::{ChunkSeriesCursor, decode_chunk_points_in_range};
    use crate::engine::chunk::{
        Chunk, ChunkHeader, ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane,
    };
    use crate::engine::encoder::Encoder;

    #[test]
    fn chunk_cursor_binary_searches_range() {
        let chunks = vec![
            chunk_with_bounds(1, 0, 9),
            chunk_with_bounds(1, 10, 19),
            chunk_with_bounds(1, 20, 29),
        ];

        let selected = ChunkSeriesCursor::new(&chunks, 12, 18).collect::<Vec<_>>();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].header.min_ts, 10);

        let selected = ChunkSeriesCursor::new(&chunks, 0, 30).collect::<Vec<_>>();
        assert_eq!(selected.len(), 3);

        let selected = ChunkSeriesCursor::new(&chunks, 30, 40).collect::<Vec<_>>();
        assert!(selected.is_empty());
    }

    #[test]
    fn decode_range_decodes_lazy_encoded_chunk() {
        let points = vec![
            ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            },
            ChunkPoint {
                ts: 3,
                value: Value::F64(3.0),
            },
        ];

        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        let chunk = Chunk {
            header: ChunkHeader {
                series_id: 7,
                lane: ValueLane::Numeric,
                point_count: points.len() as u16,
                min_ts: 1,
                max_ts: 3,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points: Vec::new(),
            encoded_payload: encoded.payload,
        };

        let decoded = decode_chunk_points_in_range(&chunk, 2, 4).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], crate::DataPoint::new(2, 2.0));
        assert_eq!(decoded[1], crate::DataPoint::new(3, 3.0));
    }

    fn chunk_with_bounds(series_id: u64, min_ts: i64, max_ts: i64) -> Chunk {
        Chunk {
            header: ChunkHeader {
                series_id,
                lane: ValueLane::Numeric,
                point_count: 1,
                min_ts,
                max_ts,
                ts_codec: TimestampCodecId::DeltaVarint,
                value_codec: ValueCodecId::ConstantRle,
            },
            points: vec![ChunkPoint {
                ts: min_ts,
                value: Value::F64(1.0),
            }],
            encoded_payload: Vec::new(),
        }
    }
}
