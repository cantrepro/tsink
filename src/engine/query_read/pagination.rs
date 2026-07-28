use super::*;

#[derive(Debug, Clone, Copy)]
pub(in crate::engine) struct RawSeriesPagination {
    pub(super) offset: u64,
    pub(super) limit: Option<usize>,
}

impl RawSeriesPagination {
    pub(in crate::engine) fn new(offset: u64, limit: Option<usize>) -> Self {
        Self { offset, limit }
    }

    pub(super) fn rows_consumed(self, total_rows: usize) -> usize {
        let offset = usize::try_from(self.offset).unwrap_or(usize::MAX);
        match self.limit {
            Some(limit) => total_rows.min(offset.saturating_add(limit)),
            None => total_rows,
        }
    }
}

#[derive(Debug)]
pub(in super::super) struct RawSeriesScanPage {
    pub(in super::super) points: Vec<DataPoint>,
    pub(in super::super) final_rows_seen: u64,
    pub(in super::super) reached_end: bool,
    pub(in super::super) stats: PersistedTierFetchStats,
    pub(in super::super) query_reservation: Option<crate::QueryMemoryReservation>,
}

impl Default for RawSeriesScanPage {
    fn default() -> Self {
        Self {
            points: Vec::new(),
            final_rows_seen: 0,
            reached_end: true,
            stats: PersistedTierFetchStats::default(),
            query_reservation: None,
        }
    }
}

impl RawSeriesScanPage {
    pub(in super::super) fn take_query_reservation(
        &mut self,
    ) -> Option<crate::QueryMemoryReservation> {
        self.query_reservation.take()
    }

    #[cfg(test)]
    pub(in super::super) fn reserved_memory_bytes(&self) -> u64 {
        self.query_reservation
            .as_ref()
            .map_or(0, crate::QueryMemoryReservation::bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SortedSeriesDedupeMode {
    None,
    Timestamp,
    Exact,
}

pub(super) struct SortedSeriesPageCollector<'a> {
    retention_cutoff: Option<i64>,
    tombstone_ranges: Option<&'a [tombstone::TombstoneRange]>,
    dedupe_mode: SortedSeriesDedupeMode,
    skip_remaining: u64,
    take_remaining: Option<usize>,
    final_rows_seen: u64,
    points: Vec<DataPoint>,
    pending: Option<DataPoint>,
}

impl<'a> SortedSeriesPageCollector<'a> {
    pub(super) fn new(
        retention_cutoff: Option<i64>,
        tombstone_ranges: Option<&'a [tombstone::TombstoneRange]>,
        dedupe_mode: SortedSeriesDedupeMode,
        pagination: RawSeriesPagination,
        output_capacity: usize,
    ) -> Self {
        Self {
            retention_cutoff,
            tombstone_ranges,
            dedupe_mode,
            skip_remaining: pagination.offset,
            take_remaining: pagination.limit,
            final_rows_seen: 0,
            points: Vec::with_capacity(output_capacity),
            pending: None,
        }
    }

    pub(super) fn push(&mut self, point: DataPoint) -> bool {
        if self
            .retention_cutoff
            .is_some_and(|cutoff| point.timestamp < cutoff)
        {
            return false;
        }

        match self.dedupe_mode {
            SortedSeriesDedupeMode::None => self.emit(point),
            SortedSeriesDedupeMode::Timestamp => match self.pending.as_mut() {
                Some(pending) if pending.timestamp == point.timestamp => {
                    *pending = point;
                    false
                }
                Some(_) => self.push_after_pending(point),
                None => {
                    self.pending = Some(point);
                    false
                }
            },
            SortedSeriesDedupeMode::Exact => match self.pending.as_ref() {
                Some(pending)
                    if pending.timestamp == point.timestamp && pending.value == point.value =>
                {
                    false
                }
                Some(_) => self.push_after_pending(point),
                None => {
                    self.pending = Some(point);
                    false
                }
            },
        }
    }

    /// Flushes a deduplication-pending point and reports whether it is an unreturned lookahead row.
    pub(super) fn finish(&mut self) -> bool {
        self.flush_pending()
    }

    pub(super) fn final_rows_seen(&self) -> u64 {
        self.final_rows_seen
    }

    pub(super) fn into_points(self) -> Vec<DataPoint> {
        self.points
    }

    fn flush_pending(&mut self) -> bool {
        self.pending.take().is_some_and(|point| self.emit(point))
    }

    fn push_after_pending(&mut self, point: DataPoint) -> bool {
        if self.flush_pending() {
            true
        } else {
            self.pending = Some(point);
            false
        }
    }

    fn emit(&mut self, point: DataPoint) -> bool {
        if self
            .tombstone_ranges
            .is_some_and(|ranges| tombstone::timestamp_is_tombstoned(point.timestamp, ranges))
        {
            return false;
        }

        if self.skip_remaining > 0 {
            self.final_rows_seen = self.final_rows_seen.saturating_add(1);
            self.skip_remaining = self.skip_remaining.saturating_sub(1);
            return false;
        }

        if self.take_remaining == Some(0) {
            return true;
        }

        self.final_rows_seen = self.final_rows_seen.saturating_add(1);
        self.points.push(point);
        if let Some(remaining) = self.take_remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }

        false
    }
}
