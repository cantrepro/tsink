//! Concurrency utilities for tsink.

use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

        self.condvar.notify_one();
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
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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

    #[test]
    fn semaphore_with_zero_permits_clamps_to_one() {
        let sem = Semaphore::new(0);
        assert_eq!(sem.capacity(), 1);
        assert_eq!(sem.available_permits(), 1);
    }

    #[test]
    fn try_acquire_for_returns_timeout_when_no_permits_are_available() {
        let sem = Semaphore::new(1);
        let _held = sem.acquire();

        let result = sem.try_acquire_for(Duration::from_millis(10));
        assert!(matches!(
            result,
            Err(TsinkError::WriteTimeout { workers: 1, .. })
        ));
    }

    #[test]
    fn try_acquire_for_zero_timeout_behaves_like_non_blocking_try() {
        let sem = Semaphore::new(1);
        let _held = sem.acquire();

        let result = sem.try_acquire_for(Duration::ZERO);
        assert!(matches!(
            result,
            Err(TsinkError::WriteTimeout {
                timeout_ms: 0,
                workers: 1
            })
        ));
    }

    #[test]
    fn try_acquire_for_succeeds_when_permit_is_released_before_deadline() {
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.acquire();

        let waiter = {
            let sem = Arc::clone(&sem);
            thread::spawn(move || sem.try_acquire_for(Duration::from_millis(200)).is_ok())
        };

        thread::sleep(Duration::from_millis(25));
        drop(held);

        assert!(waiter.join().unwrap());
    }

    #[test]
    fn acquire_all_returns_all_permits_and_releases_on_drop() {
        let sem = Semaphore::new(3);
        let guards = sem.acquire_all(Duration::from_millis(20)).unwrap();

        assert_eq!(guards.len(), 3);
        assert_eq!(sem.available_permits(), 0);

        drop(guards);
        assert_eq!(sem.available_permits(), 3);
    }

    #[test]
    fn acquire_blocks_until_permit_is_released() {
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.acquire();

        let (tx, rx) = mpsc::channel();
        let waiter = {
            let sem = Arc::clone(&sem);
            thread::spawn(move || {
                let _guard = sem.acquire();
                tx.send(()).unwrap();
            })
        };

        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        drop(held);
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_ok());
        waiter.join().unwrap();
    }

    #[test]
    fn acquire_all_times_out_if_any_permit_remains_unavailable() {
        let sem = Semaphore::new(2);
        let _held = sem.acquire();

        let result = sem.acquire_all(Duration::from_millis(10));
        assert!(matches!(
            result,
            Err(TsinkError::WriteTimeout { workers: 2, .. })
        ));
    }
}
