use std::collections::{BTreeMap, BTreeSet};

use super::chunk::{TimestampCodecId, ValueCodecId, ValueLane};
use super::series::SeriesId;

#[derive(Debug, Clone)]
pub struct ChunkIndexEntry {
    pub series_id: SeriesId,
    pub min_ts: i64,
    pub max_ts: i64,
    pub chunk_offset: u64,
    pub chunk_len: u32,
    pub point_count: u16,
    pub lane: ValueLane,
    pub ts_codec: TimestampCodecId,
    pub value_codec: ValueCodecId,
    pub level: u8,
}

#[derive(Debug, Default)]
pub struct ChunkIndex {
    pub entries: Vec<ChunkIndexEntry>,
}

impl ChunkIndex {
    pub fn add_entry(&mut self, entry: ChunkIndexEntry) {
        self.entries.push(entry);
    }

    pub fn finalize(&mut self) {
        self.entries.sort_by(|a, b| {
            (a.series_id, a.min_ts, a.max_ts, a.chunk_offset).cmp(&(
                b.series_id,
                b.min_ts,
                b.max_ts,
                b.chunk_offset,
            ))
        });
    }

    fn series_bounds(&self, series_id: SeriesId) -> std::ops::Range<usize> {
        let start = self
            .entries
            .partition_point(|entry| entry.series_id < series_id);
        let end = self
            .entries
            .partition_point(|entry| entry.series_id <= series_id);
        start..end
    }

    pub fn range_for_series(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
    ) -> Vec<&ChunkIndexEntry> {
        if start >= end {
            return Vec::new();
        }

        let bounds = self.series_bounds(series_id);
        self.entries[bounds]
            .iter()
            .filter(|entry| entry.max_ts >= start && entry.min_ts < end)
            .collect()
    }

    pub fn entries_for_series(&self, series_id: SeriesId) -> Vec<&ChunkIndexEntry> {
        self.entries[self.series_bounds(series_id)].iter().collect()
    }
}

#[derive(Debug, Default)]
pub struct PostingsIndex {
    by_label: BTreeMap<String, BTreeMap<String, BTreeSet<SeriesId>>>,
}

impl PostingsIndex {
    pub fn insert(
        &mut self,
        label_name: impl Into<String>,
        label_value: impl Into<String>,
        series_id: SeriesId,
    ) {
        let label_name = label_name.into();
        let label_value = label_value.into();
        self.by_label
            .entry(label_name)
            .or_default()
            .entry(label_value)
            .or_default()
            .insert(series_id);
    }

    pub fn get(&self, label_name: &str, label_value: &str) -> Option<&BTreeSet<SeriesId>> {
        self.by_label.get(label_name)?.get(label_value)
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkIndex, ChunkIndexEntry, PostingsIndex};
    use crate::engine::chunk::{TimestampCodecId, ValueCodecId, ValueLane};

    fn entry(series_id: u64, min_ts: i64, max_ts: i64, chunk_offset: u64) -> ChunkIndexEntry {
        ChunkIndexEntry {
            series_id,
            min_ts,
            max_ts,
            chunk_offset,
            chunk_len: 10,
            point_count: 1,
            lane: ValueLane::Numeric,
            ts_codec: TimestampCodecId::DeltaVarint,
            value_codec: ValueCodecId::ConstantRle,
            level: 0,
        }
    }

    #[test]
    fn range_for_series_uses_exclusive_end_boundary() {
        let mut index = ChunkIndex::default();
        index.add_entry(entry(7, 0, 9, 0));
        index.add_entry(entry(7, 10, 19, 1));
        index.add_entry(entry(7, 20, 29, 2));
        index.finalize();

        let selected = index.range_for_series(7, 10, 20);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].min_ts, 10);
        assert_eq!(selected[0].max_ts, 19);
    }

    #[test]
    fn range_for_series_is_empty_for_non_positive_range() {
        let mut index = ChunkIndex::default();
        index.add_entry(entry(7, 10, 19, 0));
        index.finalize();

        assert!(index.range_for_series(7, 10, 10).is_empty());
        assert!(index.range_for_series(7, 11, 10).is_empty());
    }

    #[test]
    fn postings_index_get_returns_inserted_series_ids() {
        let mut index = PostingsIndex::default();
        index.insert("region", "use1", 11);
        index.insert("region", "use1", 12);
        index.insert("region", "usw2", 13);

        let postings = index.get("region", "use1").expect("postings for use1");
        assert_eq!(postings.len(), 2);
        assert!(postings.contains(&11));
        assert!(postings.contains(&12));
        assert!(index.get("region", "missing").is_none());
        assert!(index.get("missing", "use1").is_none());
    }
}
