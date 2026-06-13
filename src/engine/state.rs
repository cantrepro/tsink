#[path = "state/active.rs"]
mod active;
#[path = "state/persisted.rs"]
mod persisted;
#[path = "state/sealed.rs"]
mod sealed;
#[path = "state/snapshot.rs"]
mod snapshot;

pub(super) use active::{
    plan_partition_head_open, ActivePartitionHead, ActiveSeriesState, PartitionHeadOpenAction,
};
pub(super) use persisted::{
    PersistedChunkRef, PersistedIndexState, PersistedSegmentState, PersistedSegmentTimeBucketIndex,
    PersistedSeriesTimeRangeSummary,
};
pub(super) use sealed::SealedChunkKey;
#[cfg(test)]
pub(in crate::engine) use snapshot::{
    active_series_snapshot_flatten_count, active_series_snapshot_point_count,
    reset_active_series_snapshot_flatten_count, reset_active_series_snapshot_point_count,
};
pub(super) use snapshot::{ActiveSeriesSnapshot, ActiveSeriesSnapshotCursor};

pub(super) const SERIES_VISIBILITY_SUMMARY_MAX_RANGES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SeriesVisibilityRangeSummary {
    pub(super) min_ts: i64,
    pub(super) max_ts: i64,
    pub(super) exact: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct SeriesVisibilitySummary {
    pub(super) latest_visible_timestamp: Option<i64>,
    pub(super) latest_bounded_visible_timestamp: Option<i64>,
    pub(super) exhaustive_floor_inclusive: Option<i64>,
    pub(super) truncated_before_floor: bool,
    pub(super) ranges: Vec<SeriesVisibilityRangeSummary>,
}

pub(super) const NUMERIC_LANE_ROOT: &str = "lane_numeric";
pub(super) const BLOB_LANE_ROOT: &str = "lane_blob";
pub(super) const WAL_DIR_NAME: &str = "wal";
pub(super) const SERIES_INDEX_FILE_NAME: &str = "series_index.bin";
