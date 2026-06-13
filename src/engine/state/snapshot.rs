use super::super::chunk::ChunkBuilderSnapshotCursor;
use super::super::*;

#[cfg(test)]
std::thread_local! {
    static ACTIVE_SERIES_SNAPSHOT_FLATTEN_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static ACTIVE_SERIES_SNAPSHOT_POINT_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[derive(Debug, Clone, Default)]
pub(in crate::engine::storage_engine) struct ActivePartitionSnapshot {
    points: chunk::ChunkBuilderSnapshot,
}

impl ActivePartitionSnapshot {
    pub(in crate::engine::storage_engine) fn from_points(
        points: chunk::ChunkBuilderSnapshot,
    ) -> Self {
        Self { points }
    }

    pub(in crate::engine::storage_engine) fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(in crate::engine::storage_engine) fn point_count(&self) -> usize {
        self.points.len()
    }

    pub(in crate::engine::storage_engine) fn iter_points(
        &self,
    ) -> impl Iterator<Item = &ChunkPoint> + '_ {
        self.points.iter_points()
    }

    pub(in crate::engine::storage_engine) fn into_cursor(self) -> ChunkBuilderSnapshotCursor {
        self.points.into_cursor()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(in crate::engine::storage_engine) fn into_points(self) -> Vec<ChunkPoint> {
        self.points.into_points()
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::engine::storage_engine) struct ActiveSeriesSnapshot {
    partition_heads: Vec<ActivePartitionSnapshot>,
}

pub(in crate::engine::storage_engine) struct ActiveSeriesSnapshotCursor {
    partition_heads: std::vec::IntoIter<ActivePartitionSnapshot>,
    current_partition: Option<ChunkBuilderSnapshotCursor>,
}

impl ActiveSeriesSnapshot {
    pub(in crate::engine::storage_engine) fn from_partition_heads(
        partition_heads: Vec<ActivePartitionSnapshot>,
    ) -> Self {
        Self { partition_heads }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(in crate::engine::storage_engine) fn point_count(&self) -> usize {
        self.partition_heads
            .iter()
            .map(ActivePartitionSnapshot::point_count)
            .sum()
    }

    pub(in crate::engine::storage_engine) fn iter_points_in_partition_order(
        &self,
    ) -> impl Iterator<Item = &ChunkPoint> + '_ {
        self.partition_heads
            .iter()
            .flat_map(ActivePartitionSnapshot::iter_points)
    }

    pub(in crate::engine::storage_engine) fn into_cursor(self) -> ActiveSeriesSnapshotCursor {
        ActiveSeriesSnapshotCursor {
            partition_heads: self.partition_heads.into_iter(),
            current_partition: None,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(in crate::engine::storage_engine) fn into_points_in_partition_order(
        self,
    ) -> Vec<ChunkPoint> {
        #[cfg(test)]
        ACTIVE_SERIES_SNAPSHOT_FLATTEN_COUNT.with(|count| {
            count.set(count.get().saturating_add(1));
        });

        let mut points = Vec::with_capacity(self.point_count());
        for partition in self.partition_heads {
            points.extend(partition.into_points());
        }
        points
    }
}

impl ActiveSeriesSnapshotCursor {
    fn ensure_head(&mut self) -> Option<&ChunkPoint> {
        loop {
            if self.current_partition.is_none() {
                self.current_partition = Some(self.partition_heads.next()?.into_cursor());
            }

            if self
                .current_partition
                .as_ref()
                .and_then(ChunkBuilderSnapshotCursor::peek)
                .is_some()
            {
                return self
                    .current_partition
                    .as_ref()
                    .and_then(ChunkBuilderSnapshotCursor::peek);
            }

            self.current_partition = None;
        }
    }

    pub(in crate::engine::storage_engine) fn peek(&mut self) -> Option<&ChunkPoint> {
        self.ensure_head()
    }

    pub(in crate::engine::storage_engine) fn advance(&mut self) -> bool {
        if self.ensure_head().is_none() {
            return false;
        }

        self.current_partition
            .as_mut()
            .expect("active snapshot cursor must have a partition after ensure_head")
            .advance()
    }
}

#[cfg(test)]
pub(in crate::engine) fn reset_active_series_snapshot_flatten_count() {
    ACTIVE_SERIES_SNAPSHOT_FLATTEN_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(in crate::engine) fn active_series_snapshot_flatten_count() -> usize {
    ACTIVE_SERIES_SNAPSHOT_FLATTEN_COUNT.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(in crate::engine) fn reset_active_series_snapshot_point_count() {
    ACTIVE_SERIES_SNAPSHOT_POINT_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(in crate::engine) fn active_series_snapshot_point_count() -> usize {
    ACTIVE_SERIES_SNAPSHOT_POINT_COUNT.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(in crate::engine::storage_engine) fn record_active_series_snapshot_point_count(
    point_count: usize,
) {
    ACTIVE_SERIES_SNAPSHOT_POINT_COUNT.with(|count| {
        count.set(count.get().saturating_add(point_count));
    });
}
