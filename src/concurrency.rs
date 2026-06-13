//! Concurrency utilities for tsink.

use crossbeam_channel::{Receiver, Sender, bounded};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use crate::{Result, TsinkError};

/// A semaphore implementation for limiting concurrent operations.
#[derive(Clone)]
pub struct Semaphore {
    permits: Arc<AtomicUsize>,
    #[allow(dead_code)]
    max_permits: usize,
    condvar: Arc<Condvar>,
    mutex: Arc<Mutex<()>>,
}

impl Semaphore {
    /// Creates a new semaphore with the specified number of permits.
    pub fn new(permits: usize) -> Self {
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
                // Wait for a permit to become available using condition variable
                let mut lock = self.mutex.lock();
                while self.permits.load(Ordering::Acquire) == 0 {
                    self.condvar.wait(&mut lock);
                }
            }
        }
    }

    /// Tries to acquire a permit without blocking.
    pub fn try_acquire(&self) -> Option<SemaphoreGuard<'_>> {
        let current = self.permits.load(Ordering::Acquire);
        if current > 0 {
            if self
                .permits
                .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(SemaphoreGuard { semaphore: self });
            }
        }
        None
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

/// A pool of worker threads for concurrent task execution.
pub struct WorkerPool<T: Send + 'static> {
    workers: Vec<Worker>,
    sender: Sender<Message<T>>,
    #[allow(dead_code)]
    receiver: Arc<Mutex<Receiver<Message<T>>>>,
    shutdown: Arc<AtomicBool>,
    active_tasks: Arc<AtomicUsize>,
}

enum Message<T> {
    Task(T),
    Shutdown,
}

struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> WorkerPool<T> {
    /// Creates a new worker pool with the specified number of workers.
    pub fn new<F>(num_workers: usize, task_handler: F) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
    {
        let (sender, receiver) = bounded::<Message<T>>(num_workers * 2);
        let receiver = Arc::new(Mutex::new(receiver));
        let shutdown = Arc::new(AtomicBool::new(false));
        let active_tasks = Arc::new(AtomicUsize::new(0));
        let task_handler = Arc::new(task_handler);

        let mut workers = Vec::with_capacity(num_workers);

        for id in 0..num_workers {
            let receiver = Arc::clone(&receiver);
            let shutdown = Arc::clone(&shutdown);
            let active_tasks = Arc::clone(&active_tasks);
            let task_handler = Arc::clone(&task_handler);

            let thread = thread::Builder::new()
                .name(format!("tsink-worker-{}", id))
                .spawn(move || {
                    info!("Worker {} started", id);

                    loop {
                        if shutdown.load(Ordering::Acquire) {
                            info!("Worker {} shutting down", id);
                            break;
                        }

                        let message = {
                            let receiver = receiver.lock();
                            match receiver.recv_timeout(Duration::from_millis(100)) {
                                Ok(msg) => msg,
                                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                                    info!("Worker {} channel disconnected", id);
                                    break;
                                }
                            }
                        };

                        match message {
                            Message::Task(task) => {
                                active_tasks.fetch_add(1, Ordering::AcqRel);
                                debug!("Worker {} processing task", id);
                                task_handler(task);
                                active_tasks.fetch_sub(1, Ordering::AcqRel);
                            }
                            Message::Shutdown => {
                                info!("Worker {} received shutdown signal", id);
                                break;
                            }
                        }
                    }
                })
                .expect("Failed to spawn worker thread");

            workers.push(Worker {
                id,
                thread: Some(thread),
            });
        }

        Self {
            workers,
            sender,
            receiver,
            shutdown,
            active_tasks,
        }
    }

    /// Submits a task to the worker pool.
    #[instrument(skip(self, task))]
    pub fn submit(&self, task: T) -> Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(TsinkError::StorageShuttingDown);
        }

        self.sender
            .send(Message::Task(task))
            .map_err(|_| TsinkError::ChannelSend {
                channel: "worker_pool".to_string(),
            })?;

        Ok(())
    }

    /// Submits a task with a timeout.
    pub fn submit_with_timeout(&self, task: T, timeout: Duration) -> Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(TsinkError::StorageShuttingDown);
        }

        self.sender
            .send_timeout(Message::Task(task), timeout)
            .map_err(|e| match e {
                crossbeam_channel::SendTimeoutError::Timeout(_) => TsinkError::ChannelTimeout {
                    timeout_ms: timeout.as_millis() as u64,
                },
                crossbeam_channel::SendTimeoutError::Disconnected(_) => TsinkError::ChannelSend {
                    channel: "worker_pool".to_string(),
                },
            })?;

        Ok(())
    }

    /// Returns the number of active tasks.
    pub fn active_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Acquire)
    }

    /// Waits for all active tasks to complete with a timeout.
    pub fn wait_for_completion(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();

        while self.active_tasks.load(Ordering::Acquire) > 0 {
            if start.elapsed() > timeout {
                return Err(TsinkError::WriteTimeout {
                    timeout_ms: timeout.as_millis() as u64,
                    workers: self.workers.len(),
                });
            }
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }

    /// Shuts down the worker pool gracefully.
    pub fn shutdown(mut self) -> Result<()> {
        info!("Shutting down worker pool");
        self.shutdown.store(true, Ordering::Release);

        // Send shutdown messages to all workers
        for _ in &self.workers {
            let _ = self.sender.send(Message::Shutdown);
        }

        // Wait for all workers to finish
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                match thread.join() {
                    Ok(_) => info!("Worker {} shut down successfully", worker.id),
                    Err(_) => error!("Worker {} panicked during shutdown", worker.id),
                }
            }
        }

        Ok(())
    }
}

/// Rate limiter for controlling operation frequency.
pub struct RateLimiter {
    max_ops_per_second: usize,
    window_start: Arc<Mutex<std::time::Instant>>,
    ops_in_window: Arc<AtomicUsize>,
}

impl RateLimiter {
    /// Creates a new rate limiter with the specified operations per second limit.
    pub fn new(max_ops_per_second: usize) -> Self {
        Self {
            max_ops_per_second,
            window_start: Arc::new(Mutex::new(std::time::Instant::now())),
            ops_in_window: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Waits if necessary to maintain the rate limit.
    pub fn wait_if_needed(&self) {
        loop {
            let now = std::time::Instant::now();
            let mut window_start = self.window_start.lock();

            // Reset window if a second has passed
            if now.duration_since(*window_start) >= Duration::from_secs(1) {
                *window_start = now;
                self.ops_in_window.store(0, Ordering::Release);
            }

            let current_ops = self.ops_in_window.load(Ordering::Acquire);
            if current_ops < self.max_ops_per_second {
                self.ops_in_window.fetch_add(1, Ordering::AcqRel);
                break;
            }

            // Wait for the next window
            drop(window_start);
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Tries to acquire a permit without blocking.
    pub fn try_acquire(&self) -> bool {
        let now = std::time::Instant::now();
        let mut window_start = self.window_start.lock();

        // Reset window if a second has passed
        if now.duration_since(*window_start) >= Duration::from_secs(1) {
            *window_start = now;
            self.ops_in_window.store(1, Ordering::Release);
            return true;
        }

        let current_ops = self.ops_in_window.load(Ordering::Acquire);
        if current_ops < self.max_ops_per_second {
            self.ops_in_window.fetch_add(1, Ordering::AcqRel);
            true
        } else {
            false
        }
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

    #[test]
    fn test_worker_pool() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let pool = WorkerPool::new(4, move |value: usize| {
            counter_clone.fetch_add(value, Ordering::AcqRel);
            // Add small delay to ensure task is processed
            thread::sleep(Duration::from_millis(1));
        });

        for i in 1..=10 {
            pool.submit(i).unwrap();
        }

        // Wait a bit longer to ensure all tasks are processed
        thread::sleep(Duration::from_millis(100));
        pool.wait_for_completion(Duration::from_secs(5)).unwrap();

        let result = counter.load(Ordering::Acquire);
        // Allow some tolerance in case of timing issues
        assert!(result >= 55, "Expected at least 55, got {}", result);

        pool.shutdown().unwrap();
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(5);

        // Should allow 5 operations immediately
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }

        // 6th operation should be blocked
        assert!(!limiter.try_acquire());
    }
}
