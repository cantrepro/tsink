use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Result, TsinkError};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SeriesCreationRateSnapshot {
    pub(crate) pending: usize,
    pub(crate) committed_in_window: usize,
    pub(crate) window_start: Option<i64>,
    pub(crate) admitted_total: u64,
    pub(crate) committed_total: u64,
    pub(crate) rejections_total: u64,
}

#[derive(Debug, Default)]
struct SeriesCreationRateState {
    generation: u64,
    window_start: Option<i64>,
    pending: usize,
    committed: usize,
    admitted_total: u64,
    committed_total: u64,
    rejections_total: u64,
}

/// Concurrent fixed-window admission for newly created series.
///
/// Reservations belong to the window in which they were admitted. Advancing (or rewinding) the
/// storage clock opens a new generation; a late completion from an older generation contributes
/// only to the lifetime total and cannot corrupt the new window's counters.
#[derive(Debug)]
pub(crate) struct SeriesCreationRateLimiter {
    limit: Option<usize>,
    window_units: i64,
    window_nanos: u64,
    state: Mutex<SeriesCreationRateState>,
}

impl SeriesCreationRateLimiter {
    pub(crate) fn new(limit: Option<usize>, window_units: i64, window_nanos: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            window_units: window_units.max(1),
            window_nanos,
            state: Mutex::new(SeriesCreationRateState::default()),
        })
    }

    fn rotate_window_if_needed(&self, state: &mut SeriesCreationRateState, now: i64) {
        let rotate = state
            .window_start
            .is_none_or(|start| now < start || now.saturating_sub(start) >= self.window_units);
        if !rotate {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        state.window_start = Some(now);
        state.pending = 0;
        state.committed = 0;
    }

    pub(crate) fn reserve(
        self: &Arc<Self>,
        now: i64,
        requested: usize,
    ) -> Result<Option<SeriesCreationRateReservation>> {
        let Some(limit) = self.limit else {
            return Ok(None);
        };
        if requested == 0 {
            return Ok(None);
        }

        let mut state = self.state.lock();
        self.rotate_window_if_needed(&mut state, now);
        let current = state.committed.saturating_add(state.pending);
        if current > limit || requested > limit - current {
            state.rejections_total = state.rejections_total.saturating_add(1);
            return Err(TsinkError::CardinalityCreationRateExceeded {
                limit,
                current,
                requested,
                window_nanos: self.window_nanos,
            });
        }

        state.pending = state.pending.saturating_add(requested);
        state.admitted_total = state
            .admitted_total
            .saturating_add(u64::try_from(requested).unwrap_or(u64::MAX));
        let generation = state.generation;
        drop(state);
        Ok(Some(SeriesCreationRateReservation {
            limiter: Arc::clone(self),
            generation,
            count: requested,
            settled: false,
        }))
    }

    fn release(&self, generation: u64, count: usize) {
        let mut state = self.state.lock();
        if state.generation == generation {
            state.pending = state.pending.saturating_sub(count);
        }
    }

    fn commit(&self, generation: u64, count: usize) {
        let mut state = self.state.lock();
        if state.generation == generation {
            state.pending = state.pending.saturating_sub(count);
            state.committed = state.committed.saturating_add(count);
        }
        state.committed_total = state
            .committed_total
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
    }

    pub(crate) fn snapshot(&self, now: i64) -> SeriesCreationRateSnapshot {
        let mut state = self.state.lock();
        if self.limit.is_some() {
            self.rotate_window_if_needed(&mut state, now);
        }
        SeriesCreationRateSnapshot {
            pending: state.pending,
            committed_in_window: state.committed,
            window_start: state.window_start,
            admitted_total: state.admitted_total,
            committed_total: state.committed_total,
            rejections_total: state.rejections_total,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SeriesCreationRateReservation {
    limiter: Arc<SeriesCreationRateLimiter>,
    generation: u64,
    count: usize,
    settled: bool,
}

impl SeriesCreationRateReservation {
    /// Reconciles a conservative missing-series reservation to the number actually created.
    pub(crate) fn retain(&mut self, count: usize) {
        debug_assert!(count <= self.count);
        let count = count.min(self.count);
        let released = self.count - count;
        if released > 0 {
            self.limiter.release(self.generation, released);
            self.count = count;
        }
    }

    pub(crate) fn commit(mut self) {
        if self.count > 0 {
            self.limiter.commit(self.generation, self.count);
        }
        self.settled = true;
    }
}

impl Drop for SeriesCreationRateReservation {
    fn drop(&mut self) {
        if !self.settled && self.count > 0 {
            self.limiter.release(self.generation, self.count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_reservations_cannot_bypass_window_limit() {
        let limiter = SeriesCreationRateLimiter::new(Some(2), 10, 1_000);
        let first = limiter.reserve(100, 1).unwrap().unwrap();
        let second = limiter.reserve(100, 1).unwrap().unwrap();
        assert!(matches!(
            limiter.reserve(100, 1),
            Err(TsinkError::CardinalityCreationRateExceeded {
                limit: 2,
                current: 2,
                requested: 1,
                ..
            })
        ));
        drop(first);
        let replacement = limiter.reserve(100, 1).unwrap().unwrap();
        second.commit();
        replacement.commit();
        let snapshot = limiter.snapshot(100);
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.committed_in_window, 2);
    }

    #[test]
    fn old_generation_completion_cannot_change_new_window() {
        let limiter = SeriesCreationRateLimiter::new(Some(1), 10, 1_000);
        let old = limiter.reserve(100, 1).unwrap().unwrap();
        let current = limiter.reserve(110, 1).unwrap().unwrap();
        old.commit();
        assert_eq!(limiter.snapshot(110).committed_in_window, 0);
        current.commit();
        let snapshot = limiter.snapshot(110);
        assert_eq!(snapshot.committed_in_window, 1);
        assert_eq!(snapshot.committed_total, 2);
    }

    #[test]
    fn retained_count_releases_conservative_excess() {
        let limiter = SeriesCreationRateLimiter::new(Some(3), 10, 1_000);
        let mut reservation = limiter.reserve(100, 3).unwrap().unwrap();
        reservation.retain(1);
        let remaining = limiter.reserve(100, 2).unwrap().unwrap();
        reservation.commit();
        remaining.commit();
        assert_eq!(limiter.snapshot(100).committed_in_window, 3);
    }

    #[test]
    fn observation_rolls_an_elapsed_window_forward_without_a_write() {
        let limiter = SeriesCreationRateLimiter::new(Some(1), 10, 1_000);
        limiter.reserve(100, 1).unwrap().unwrap().commit();

        let snapshot = limiter.snapshot(110);
        assert_eq!(snapshot.window_start, Some(110));
        assert_eq!(snapshot.committed_in_window, 0);
        assert_eq!(snapshot.committed_total, 1);
    }

    #[test]
    fn maximum_limit_does_not_wrap_or_saturate_requested_capacity() {
        let limiter = SeriesCreationRateLimiter::new(Some(usize::MAX), 10, 1_000);
        let reservation = limiter.reserve(100, usize::MAX).unwrap().unwrap();

        assert!(matches!(
            limiter.reserve(100, 1),
            Err(TsinkError::CardinalityCreationRateExceeded {
                limit: usize::MAX,
                current: usize::MAX,
                requested: 1,
                ..
            })
        ));

        drop(reservation);
        assert!(limiter.reserve(100, 1).unwrap().is_some());
    }
}
