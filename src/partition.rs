//! Partition implementations for tsink.

use crate::{DataPoint, Label, Result, Row};
use std::sync::Arc;

/// A partition is a chunk of time-series data with a timestamp range.
///
/// Partitions act as fully independent databases containing all data points
/// for their time range. The lifecycle is: Writable -> ReadOnly.
pub trait Partition: Send + Sync {
    /// Inserts rows into the partition.
    /// Returns outdated rows that are older than the partition's min timestamp.
    fn insert_rows(&self, rows: &[Row]) -> Result<Vec<Row>>;

    /// Inserts rows during WAL recovery.
    /// Default behavior matches normal inserts; partitions can override to skip WAL appends.
    fn insert_rows_recovery(&self, rows: &[Row]) -> Result<Vec<Row>> {
        self.insert_rows(rows)
    }

    /// Selects data points for a specific metric within the given time range.
    fn select_data_points(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>>;

    /// Selects data points for a metric regardless of labels.
    /// Returns a map of label sets to their corresponding data points.
    fn select_all_labels(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>>;

    /// Returns the minimum timestamp in the partition.
    fn min_timestamp(&self) -> i64;

    /// Returns the maximum timestamp in the partition.
    fn max_timestamp(&self) -> i64;

    /// Returns the number of data points in the partition.
    fn size(&self) -> usize;

    /// Returns true if the partition is active (writable and can be head).
    fn active(&self) -> bool;

    /// Returns true if the partition has expired and should be removed.
    fn expired(&self) -> bool;

    /// Cleans up resources managed by this partition.
    fn clean(&self) -> Result<()>;

    /// Flushes the partition's data to disk, returning the encoded data and metadata.
    /// Returns None if the partition doesn't support flushing (e.g., already on disk).
    fn flush_to_disk(&self) -> Result<Option<(Vec<u8>, crate::disk::PartitionMeta)>>;

    /// Marks a partition as being flushed, preventing new writes from being accepted.
    /// Returns `true` when the partition supports this state transition.
    fn begin_flush(&self) -> bool {
        false
    }

    /// Clears a previously established flush marker.
    fn end_flush(&self) {}
}

/// Type alias for a shared partition reference.
pub type SharedPartition = Arc<dyn Partition>;
