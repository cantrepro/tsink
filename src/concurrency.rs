//! Concurrency utilities for tsink.

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

use crate::{Result, TsinkError};

/// A semaphore implementation for limiting concurrent operations.
#[derive(Clone)]
pub struct Semaphore {
    permits: Arc<AtomicUsize>,
    max_permits: usize,
    condvar: Arc<Condvar>,
    mutex: Arc<Mutex<()>>,
}

impl Semaphore {
    /// Creates a new semaphore with the specified number of permits.
    pub fn new(permits: usize) -> Self {
        let permits = permits.max(1);
        Self {
            permits: Arc::new(AtomicUsize::new(permits)),
            max_permits: permits,
            condvar: Arc::new(Condvar::new()),
            mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Acquires a permit from the semaphore.
    #[instrument(skip(self))]
    pub fn acquire(&self) -> SemaphoreGuard<'_> {
        loop {
            let current = self.permits.load(Ordering::Acquire);
            if current > 0 {
                if self
                    .permits
                    .compare_exchange_weak(
                        current,
                        current - 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    debug!("Acquired semaphore permit, {} remaining", current - 1);
                    return SemaphoreGuard { semaphore: self };
                }
            } else {
                let mut lock = self.mutex.lock();
                while self.permits.load(Ordering::Acquire) == 0 {
                    self.condvar.wait(&mut lock);
                }
            }
        }
    }

    /// Tries to acquire a permit without blocking.
    pub fn try_acquire(&self) -> Option<SemaphoreGuard<'_>> {
        let mut current = self.permits.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return None;
            }

            match self.permits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(SemaphoreGuard { semaphore: self }),
                Err(actual) => current = actual,
            }
        }
    }

    /// Tries to acquire a permit, waiting up to the provided timeout.
    pub fn try_acquire_for(&self, timeout: Duration) -> Result<SemaphoreGuard<'_>> {
        if timeout.is_zero() {
            return self.try_acquire().ok_or(TsinkError::WriteTimeout {
                timeout_ms: 0,
                workers: self.max_permits,
            });
        }

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(guard) = self.try_acquire() {
                return Ok(guard);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(TsinkError::WriteTimeout {
                    timeout_ms: timeout.as_millis() as u64,
                    workers: self.max_permits,
                });
            }

            let mut lock = self.mutex.lock();
            while self.permits.load(Ordering::Acquire) == 0 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(TsinkError::WriteTimeout {
                        timeout_ms: timeout.as_millis() as u64,
                        workers: self.max_permits,
                    });
                }
                if self.condvar.wait_for(&mut lock, remaining).timed_out()
                    && self.permits.load(Ordering::Acquire) == 0
                {
                    return Err(TsinkError::WriteTimeout {
                        timeout_ms: timeout.as_millis() as u64,
                        workers: self.max_permits,
                    });
                }
            }
        }
    }

    /// Acquires all permits, waiting up to timeout.
    pub fn acquire_all(&self, timeout: Duration) -> Result<Vec<SemaphoreGuard<'_>>> {
        let deadline = Instant::now() + timeout;
        let mut guards = Vec::with_capacity(self.max_permits);

        for _ in 0..self.max_permits {
            let now = Instant::now();
            if now >= deadline {
                return Err(TsinkError::WriteTimeout {
                    timeout_ms: timeout.as_millis() as u64,
                    workers: self.max_permits,
                });
            }

            let remaining = deadline.saturating_duration_since(now);
            let guard = self.try_acquire_for(remaining)?;
            guards.push(guard);
        }

        Ok(guards)
    }

    /// Returns the semaphore capacity.
    pub fn capacity(&self) -> usize {
        self.max_permits
    }

    /// Releases a permit back to the semaphore.
    fn release(&self) {
        let previous = self.permits.fetch_add(1, Ordering::AcqRel);
        debug!("Released semaphore permit, {} now available", previous + 1);

        // Wake up all waiters to avoid starvation
        self.condvar.notify_all();
    }

    /// Returns the number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Acquire)
    }
}

/// Guard that automatically releases a semaphore permit when dropped.
pub struct SemaphoreGuard<'a> {
    semaphore: &'a Semaphore,
}

impl<'a> Drop for SemaphoreGuard<'a> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semaphore() {
        let sem = Semaphore::new(2);

        let _guard1 = sem.acquire();
        assert_eq!(sem.available_permits(), 1);

        let _guard2 = sem.acquire();
        assert_eq!(sem.available_permits(), 0);

        assert!(sem.try_acquire().is_none());

        drop(_guard1);
        assert_eq!(sem.available_permits(), 1);
    }
}
