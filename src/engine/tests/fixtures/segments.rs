use super::super::*;

pub(in crate::engine::storage_engine::tests) fn make_persisted_numeric_chunk(
    series_id: u64,
    points: &[(i64, f64)],
) -> Chunk {
    assert!(!points.is_empty());
    let chunk_points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();
    let encoded = Encoder::encode_chunk_points(&chunk_points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: chunk_points.len() as u16,
            min_ts: chunk_points.first().unwrap().ts,
            max_ts: chunk_points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points: chunk_points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}
