use super::*;

pub(super) const MIN_BACKGROUND_WORKER_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
pub(super) enum BackgroundThreadKind {
    Compaction,
    Flush,
    PersistedRefresh,
    Rollup,
}

impl BackgroundThreadKind {
    const SHUTDOWN_ORDER: [Self; 4] = [
        Self::Compaction,
        Self::Flush,
        Self::PersistedRefresh,
        Self::Rollup,
    ];

    fn worker_name(self) -> &'static str {
        match self {
            Self::Compaction => "compaction",
            Self::Flush => "flush",
            Self::PersistedRefresh => "persisted_refresh",
            Self::Rollup => "rollup",
        }
    }
}

pub(super) fn normalize_background_worker_interval(interval: Duration) -> Duration {
    interval.max(MIN_BACKGROUND_WORKER_INTERVAL)
}

impl BackgroundWorkerRuntimeState {
    fn configure_interval(&self, interval: Duration) {
        self.interval_nanos.store(
            interval.as_nanos().min(u64::MAX.into()) as u64,
            Ordering::Relaxed,
        );
    }

    pub(super) fn record_idle_wait(&self) {
        self.idle_waits_total.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, installed: bool) -> crate::BackgroundWorkerObservabilitySnapshot {
        let interval_nanos = self.interval_nanos.load(Ordering::Relaxed);
        crate::BackgroundWorkerObservabilitySnapshot {
            installed,
            running: self.running.load(Ordering::SeqCst),
            interval_nanos: (interval_nanos > 0).then_some(interval_nanos),
            max_concurrency: 1,
            starts_total: self.starts_total.load(Ordering::Relaxed),
            exits_total: self.exits_total.load(Ordering::Relaxed),
            notifications_total: self.notifications_total.load(Ordering::Relaxed),
            idle_waits_total: self.idle_waits_total.load(Ordering::Relaxed),
            passes_started_total: self.passes_started_total.load(Ordering::Relaxed),
            passes_completed_total: self.passes_completed_total.load(Ordering::Relaxed),
            shutdown_joins_total: self.shutdown_joins_total.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct BackgroundWorkerRunGuard {
    runtime: Arc<BackgroundWorkerRuntimeState>,
}

impl BackgroundWorkerRunGuard {
    pub(super) fn new(runtime: Arc<BackgroundWorkerRuntimeState>) -> Self {
        runtime.running.store(true, Ordering::SeqCst);
        runtime.starts_total.fetch_add(1, Ordering::Relaxed);
        Self { runtime }
    }
}

impl Drop for BackgroundWorkerRunGuard {
    fn drop(&mut self) {
        self.runtime.running.store(false, Ordering::SeqCst);
        self.runtime.exits_total.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) struct BackgroundWorkerPassGuard<'a> {
    runtime: &'a BackgroundWorkerRuntimeState,
}

impl<'a> BackgroundWorkerPassGuard<'a> {
    pub(super) fn new(runtime: &'a BackgroundWorkerRuntimeState) -> Self {
        runtime.passes_started_total.fetch_add(1, Ordering::Relaxed);
        Self { runtime }
    }
}

impl Drop for BackgroundWorkerPassGuard<'_> {
    fn drop(&mut self) {
        self.runtime
            .passes_completed_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl BackgroundWorkerSupervisorState {
    fn thread_slot(
        &self,
        worker: BackgroundThreadKind,
    ) -> &Mutex<Option<std::thread::JoinHandle<()>>> {
        match worker {
            BackgroundThreadKind::Compaction => &self.compaction_thread,
            BackgroundThreadKind::Flush => &self.flush_thread,
            BackgroundThreadKind::PersistedRefresh => &self.persisted_refresh_thread,
            BackgroundThreadKind::Rollup => &self.rollup_thread,
        }
    }

    fn runtime_state(&self, worker: BackgroundThreadKind) -> &Arc<BackgroundWorkerRuntimeState> {
        match worker {
            BackgroundThreadKind::Compaction => &self.compaction_runtime,
            BackgroundThreadKind::Flush => &self.flush_runtime,
            BackgroundThreadKind::PersistedRefresh => &self.persisted_refresh_runtime,
            BackgroundThreadKind::Rollup => &self.rollup_runtime,
        }
    }

    pub(super) fn install_thread(
        &self,
        worker: BackgroundThreadKind,
        interval: Duration,
        spawn: impl FnOnce(
            Arc<BackgroundWorkerRuntimeState>,
            Duration,
        ) -> Result<Option<std::thread::JoinHandle<()>>>,
    ) -> Result<()> {
        let mut thread = self.thread_slot(worker).lock();
        if thread.is_some() {
            return Ok(());
        }

        let interval = normalize_background_worker_interval(interval);
        let runtime = Arc::clone(self.runtime_state(worker));
        runtime.configure_interval(interval);
        *thread = spawn(runtime, interval)?;
        Ok(())
    }

    fn has_thread(&self, worker: BackgroundThreadKind) -> bool {
        self.thread_slot(worker).lock().is_some()
    }

    pub(super) fn notify_worker(&self, worker: BackgroundThreadKind) {
        if matches!(worker, BackgroundThreadKind::Flush) {
            self.flush_thread_wakeup_requested
                .store(true, Ordering::SeqCst);
        }

        let thread_slot = self.thread_slot(worker).lock();
        if let Some(thread) = thread_slot.as_ref() {
            self.runtime_state(worker)
                .notifications_total
                .fetch_add(1, Ordering::Relaxed);
            thread.thread().unpark();
        }
    }

    fn notify_all_workers(&self) {
        for worker in BackgroundThreadKind::SHUTDOWN_ORDER {
            self.notify_worker(worker);
        }
    }

    pub(super) fn take_flush_wakeup_request(&self) -> bool {
        self.flush_thread_wakeup_requested
            .swap(false, Ordering::SeqCst)
    }

    fn take_thread(&self, worker: BackgroundThreadKind) -> Option<std::thread::JoinHandle<()>> {
        self.thread_slot(worker).lock().take()
    }

    fn panic_payload_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
        let payload = match payload.downcast::<String>() {
            Ok(message) => return *message,
            Err(payload) => payload,
        };
        let payload = match payload.downcast::<&'static str>() {
            Ok(message) => return (*message).to_string(),
            Err(payload) => payload,
        };
        format!("unknown panic payload type: {:?}", payload.type_id())
    }

    fn join_thread(
        handle: std::thread::JoinHandle<()>,
        worker_name: &str,
        runtime: &BackgroundWorkerRuntimeState,
    ) -> Result<()> {
        if handle.thread().id() == std::thread::current().id() {
            return Ok(());
        }
        let result = handle.join().map_err(|payload| {
            TsinkError::Other(format!(
                "{worker_name} worker panicked: {}",
                Self::panic_payload_message(payload)
            ))
        });
        runtime.shutdown_joins_total.fetch_add(1, Ordering::Relaxed);
        result
    }

    fn join_all_threads(&self) -> Result<()> {
        let started = Instant::now();
        let mut first_error = None;
        for worker in BackgroundThreadKind::SHUTDOWN_ORDER {
            if let Some(thread) = self.take_thread(worker) {
                if let Err(err) = Self::join_thread(
                    thread,
                    worker.worker_name(),
                    self.runtime_state(worker).as_ref(),
                ) {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        let result = match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        };
        self.shutdown_join_wait_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
        result
    }

    pub(in crate::engine::storage_engine) fn record_close_attempt(&self) -> Instant {
        self.close_attempts_total.fetch_add(1, Ordering::Relaxed);
        Instant::now()
    }

    pub(in crate::engine::storage_engine) fn record_close_result(
        &self,
        started: Instant,
        success: bool,
    ) {
        if success {
            self.close_success_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.close_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        self.close_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
    }

    pub(in crate::engine::storage_engine) fn record_close_coordination_wait(
        &self,
        started: Instant,
        timed_out: bool,
    ) {
        self.close_coordination_wait_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
        if timed_out {
            self.close_coordination_timeouts_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(in crate::engine::storage_engine) fn record_close_compaction_passes(&self, passes: usize) {
        self.close_compaction_passes_total
            .fetch_add(u64::try_from(passes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(in crate::engine::storage_engine) fn observability_snapshot(
        &self,
    ) -> crate::BackgroundWorkObservabilitySnapshot {
        let flush = self
            .flush_runtime
            .snapshot(self.has_thread(BackgroundThreadKind::Flush));
        let compaction = self
            .compaction_runtime
            .snapshot(self.has_thread(BackgroundThreadKind::Compaction));
        let persisted_refresh = self
            .persisted_refresh_runtime
            .snapshot(self.has_thread(BackgroundThreadKind::PersistedRefresh));
        let rollup = self
            .rollup_runtime
            .snapshot(self.has_thread(BackgroundThreadKind::Rollup));
        let workers = [flush, compaction, persisted_refresh, rollup];
        crate::BackgroundWorkObservabilitySnapshot {
            max_threads: workers
                .iter()
                .filter(|worker| worker.interval_nanos.is_some())
                .count() as u64,
            installed_threads: workers.iter().filter(|worker| worker.installed).count() as u64,
            running_threads: workers.iter().filter(|worker| worker.running).count() as u64,
            close_attempts_total: self.close_attempts_total.load(Ordering::Relaxed),
            close_success_total: self.close_success_total.load(Ordering::Relaxed),
            close_errors_total: self.close_errors_total.load(Ordering::Relaxed),
            close_coordination_wait_nanos_total: self
                .close_coordination_wait_nanos_total
                .load(Ordering::Relaxed),
            close_coordination_timeouts_total: self
                .close_coordination_timeouts_total
                .load(Ordering::Relaxed),
            close_compaction_passes_total: self
                .close_compaction_passes_total
                .load(Ordering::Relaxed),
            close_compaction_pass_limit: u64::try_from(super::super::CLOSE_COMPACTION_MAX_PASSES)
                .unwrap_or(u64::MAX),
            close_duration_nanos_total: self.close_duration_nanos_total.load(Ordering::Relaxed),
            shutdown_join_wait_nanos_total: self
                .shutdown_join_wait_nanos_total
                .load(Ordering::Relaxed),
            flush,
            compaction,
            persisted_refresh,
            rollup,
        }
    }
}

impl ChunkStorage {
    pub(in super::super) fn notify_background_threads(&self) {
        self.background.notify_all_workers();
    }

    pub(in super::super) fn notify_compaction_thread(&self) {
        self.background
            .notify_worker(BackgroundThreadKind::Compaction);
    }

    pub(in super::super) fn notify_flush_thread(&self) {
        self.background.notify_worker(BackgroundThreadKind::Flush);
    }

    pub(in super::super) fn notify_persisted_refresh_thread(&self) {
        self.background
            .notify_worker(BackgroundThreadKind::PersistedRefresh);
    }

    pub(in super::super) fn notify_rollup_thread(&self) {
        self.background.notify_worker(BackgroundThreadKind::Rollup);
    }

    pub(in super::super) fn join_background_threads(&self) -> Result<()> {
        self.background.join_all_threads()
    }

    pub(in super::super) fn has_persisted_refresh_thread(&self) -> bool {
        self.background
            .has_thread(BackgroundThreadKind::PersistedRefresh)
    }

    pub(in super::super) fn start_close_transition(&self) -> Result<()> {
        if self
            .coordination
            .lifecycle
            .compare_exchange(
                STORAGE_OPEN,
                STORAGE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(TsinkError::StorageClosed);
        }

        self.notify_background_threads();
        Ok(())
    }

    pub(in super::super) fn finish_close_transition(
        &self,
        mut close_result: Result<()>,
    ) -> Result<()> {
        if close_result.is_ok() {
            self.coordination
                .lifecycle
                .store(STORAGE_CLOSED, Ordering::SeqCst);
            self.notify_background_threads();
            let join_result = self.join_background_threads();
            // Keep the data-path lease until every owned worker has stopped. A worker may still
            // be completing filesystem I/O after observing the closed lifecycle; releasing the
            // lease before join would let another process open the same directory concurrently.
            self.release_data_path_process_lock();
            if let Err(err) = join_result {
                close_result = Err(err);
            }
        } else {
            self.coordination
                .lifecycle
                .store(STORAGE_OPEN, Ordering::SeqCst);
            self.notify_background_threads();
        }

        close_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::mpsc;

    fn test_background_supervisor() -> BackgroundWorkerSupervisorState {
        BackgroundWorkerSupervisorState {
            compaction_thread: Mutex::new(None),
            compaction_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            flush_thread: Mutex::new(None),
            flush_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            flush_thread_wakeup_requested: AtomicBool::new(false),
            persisted_refresh_thread: Mutex::new(None),
            persisted_refresh_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            rollup_thread: Mutex::new(None),
            rollup_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            close_attempts_total: AtomicU64::new(0),
            close_success_total: AtomicU64::new(0),
            close_errors_total: AtomicU64::new(0),
            close_coordination_wait_nanos_total: AtomicU64::new(0),
            close_coordination_timeouts_total: AtomicU64::new(0),
            close_compaction_passes_total: AtomicU64::new(0),
            close_duration_nanos_total: AtomicU64::new(0),
            shutdown_join_wait_nanos_total: AtomicU64::new(0),
            compaction_interval: Duration::from_secs(60),
            fail_fast_enabled: false,
        }
    }

    #[test]
    fn flush_notifications_record_explicit_wakeup_requests() {
        let supervisor = test_background_supervisor();

        assert!(!supervisor.take_flush_wakeup_request());
        supervisor.notify_worker(BackgroundThreadKind::Flush);
        assert!(supervisor.take_flush_wakeup_request());
        assert!(!supervisor.take_flush_wakeup_request());
    }

    #[test]
    fn background_worker_intervals_have_a_nonzero_idle_floor() {
        assert_eq!(
            normalize_background_worker_interval(Duration::ZERO),
            MIN_BACKGROUND_WORKER_INTERVAL
        );
        assert_eq!(
            normalize_background_worker_interval(Duration::from_secs(7)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn idle_worker_parks_without_spinning_and_shutdown_reaps_it() {
        let supervisor = test_background_supervisor();
        let stop = Arc::new(AtomicBool::new(false));
        let (parked_tx, parked_rx) = mpsc::sync_channel(1);

        supervisor
            .install_thread(
                BackgroundThreadKind::Rollup,
                Duration::from_secs(60),
                |runtime, _| {
                    let stop = Arc::clone(&stop);
                    Ok(Some(std::thread::spawn(move || {
                        let _run = BackgroundWorkerRunGuard::new(Arc::clone(&runtime));
                        while !stop.load(Ordering::SeqCst) {
                            runtime.record_idle_wait();
                            let _ = parked_tx.try_send(());
                            std::thread::park();
                        }
                    })))
                },
            )
            .unwrap();

        parked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let before = supervisor.observability_snapshot();
        assert_eq!(before.max_threads, 1);
        assert_eq!(before.installed_threads, 1);
        assert_eq!(before.running_threads, 1);
        assert_eq!(before.rollup.idle_waits_total, 1);
        assert_eq!(before.rollup.passes_started_total, 0);

        for _ in 0..1_000 {
            std::thread::yield_now();
        }
        let still_idle = supervisor.observability_snapshot();
        assert_eq!(still_idle.rollup.idle_waits_total, 1);
        assert_eq!(still_idle.rollup.passes_started_total, 0);

        stop.store(true, Ordering::SeqCst);
        supervisor.notify_worker(BackgroundThreadKind::Rollup);
        supervisor.join_all_threads().unwrap();

        let closed = supervisor.observability_snapshot();
        assert_eq!(closed.installed_threads, 0);
        assert_eq!(closed.running_threads, 0);
        assert_eq!(closed.rollup.starts_total, 1);
        assert_eq!(closed.rollup.exits_total, 1);
        assert_eq!(closed.rollup.notifications_total, 1);
        assert_eq!(closed.rollup.shutdown_joins_total, 1);
    }

    #[test]
    fn background_worker_slots_install_only_once() {
        let supervisor = test_background_supervisor();
        let spawn_count = AtomicUsize::new(0);

        supervisor
            .install_thread(
                BackgroundThreadKind::Flush,
                Duration::from_secs(60),
                |runtime, _| {
                    spawn_count.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(std::thread::spawn(move || {
                        let _run = BackgroundWorkerRunGuard::new(runtime);
                    })))
                },
            )
            .unwrap();
        supervisor
            .install_thread(
                BackgroundThreadKind::Flush,
                Duration::from_secs(60),
                |runtime, _| {
                    spawn_count.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(std::thread::spawn(move || {
                        let _run = BackgroundWorkerRunGuard::new(runtime);
                    })))
                },
            )
            .unwrap();

        assert_eq!(spawn_count.load(Ordering::SeqCst), 1);
        supervisor.join_all_threads().unwrap();
    }

    #[test]
    fn join_errors_include_worker_name() {
        let supervisor = test_background_supervisor();

        supervisor
            .install_thread(
                BackgroundThreadKind::Compaction,
                Duration::from_secs(60),
                |runtime, _| {
                    Ok(Some(std::thread::spawn(move || {
                        let _run = BackgroundWorkerRunGuard::new(runtime);
                        panic!("boom")
                    })))
                },
            )
            .unwrap();

        let err = supervisor.join_all_threads().unwrap_err();
        let TsinkError::Other(message) = err else {
            panic!("expected panic join error");
        };
        assert!(message.contains("compaction worker panicked"));
        assert!(message.contains("boom"));
    }

    #[test]
    fn panicked_worker_does_not_skip_remaining_shutdown_joins() {
        let supervisor = test_background_supervisor();

        supervisor
            .install_thread(
                BackgroundThreadKind::Compaction,
                Duration::from_secs(60),
                |runtime, _| {
                    Ok(Some(std::thread::spawn(move || {
                        let _run = BackgroundWorkerRunGuard::new(runtime);
                        panic!("first worker failed")
                    })))
                },
            )
            .unwrap();
        supervisor
            .install_thread(
                BackgroundThreadKind::Flush,
                Duration::from_secs(60),
                |runtime, _| {
                    Ok(Some(std::thread::spawn(move || {
                        let _run = BackgroundWorkerRunGuard::new(runtime);
                    })))
                },
            )
            .unwrap();

        let error = supervisor.join_all_threads().unwrap_err();
        assert!(error.to_string().contains("compaction worker panicked"));
        let snapshot = supervisor.observability_snapshot();
        assert_eq!(snapshot.installed_threads, 0);
        assert_eq!(snapshot.running_threads, 0);
        assert_eq!(snapshot.compaction.shutdown_joins_total, 1);
        assert_eq!(snapshot.flush.shutdown_joins_total, 1);
    }
}
