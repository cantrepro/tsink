use super::{
    elapsed_nanos_u64, Arc, AtomicBool, AtomicU8, BackgroundWorkerSupervisorState, ChunkStorage,
    Compactor, Duration, Instant, Mutex, Ordering, PendingPersistedSegmentDiff, Result,
    StorageObservabilityCounters, StorageRuntimeMode, TsinkError, DEFAULT_FLUSH_INTERVAL,
    STORAGE_CLOSED, STORAGE_CLOSING, STORAGE_OPEN,
};

#[path = "runtime/supervision.rs"]
mod supervision;

use self::supervision::BackgroundThreadKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushPipelinePolicy {
    Foreground,
    BackgroundEligibleOnly,
    BackgroundBounded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackgroundWorkerFlow {
    Continue,
    Pause(Duration),
    Exit,
}

enum BackgroundWorkerPass<G> {
    Ready(G),
    Pause(Duration),
    Exit,
}

struct BackgroundWorkerControl<'a> {
    lifecycle: &'a AtomicU8,
    observability: &'a StorageObservabilityCounters,
    fail_fast_enabled: bool,
}

impl<'a> BackgroundWorkerControl<'a> {
    fn new(
        lifecycle: &'a AtomicU8,
        observability: &'a StorageObservabilityCounters,
        fail_fast_enabled: bool,
    ) -> Self {
        Self {
            lifecycle,
            observability,
            fail_fast_enabled,
        }
    }

    fn for_storage(storage: &'a ChunkStorage) -> Self {
        let coordination = storage.coordination_context();
        Self::new(
            coordination.lifecycle,
            storage.observability.as_ref(),
            coordination.background_fail_fast,
        )
    }

    fn flow_for_state(
        lifecycle_state: u8,
        fail_fast_triggered: bool,
        pause_duration: Duration,
    ) -> BackgroundWorkerFlow {
        match lifecycle_state {
            STORAGE_OPEN => {
                if fail_fast_triggered {
                    BackgroundWorkerFlow::Exit
                } else {
                    BackgroundWorkerFlow::Continue
                }
            }
            STORAGE_CLOSED => BackgroundWorkerFlow::Exit,
            _ => BackgroundWorkerFlow::Pause(pause_duration),
        }
    }

    fn flow(&self, pause_duration: Duration) -> BackgroundWorkerFlow {
        Self::flow_for_state(
            self.lifecycle.load(Ordering::SeqCst),
            self.observability
                .health
                .fail_fast_triggered
                .load(Ordering::SeqCst),
            pause_duration,
        )
    }

    fn handle_result<T>(&self, worker: &'static str, result: Result<T>) -> BackgroundWorkerFlow {
        match result {
            Ok(_) => BackgroundWorkerFlow::Continue,
            Err(err) => {
                ChunkStorage::record_background_worker_error(
                    worker,
                    &err,
                    self.observability,
                    self.fail_fast_enabled,
                );
                if self.fail_fast_enabled {
                    BackgroundWorkerFlow::Exit
                } else {
                    BackgroundWorkerFlow::Continue
                }
            }
        }
    }
}

struct FlushWorkerSchedule {
    interval: Duration,
    next_bounded_flush_at: Instant,
}

impl FlushWorkerSchedule {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            next_bounded_flush_at: Instant::now() + interval,
        }
    }

    fn park_until_due(&self) {
        let now = Instant::now();
        if now < self.next_bounded_flush_at {
            std::thread::park_timeout(self.next_bounded_flush_at.saturating_duration_since(now));
        }
    }

    fn next_policy(&mut self, now: Instant, explicit_wakeup: bool) -> Option<FlushPipelinePolicy> {
        if now >= self.next_bounded_flush_at {
            self.next_bounded_flush_at = now + self.interval;
            Some(FlushPipelinePolicy::BackgroundBounded)
        } else if explicit_wakeup {
            Some(FlushPipelinePolicy::BackgroundEligibleOnly)
        } else {
            None
        }
    }
}

impl ChunkStorage {
    fn begin_background_worker_pass<G, L>(
        control: &BackgroundWorkerControl<'_>,
        pause_duration: Duration,
        acquire_guard: L,
    ) -> BackgroundWorkerPass<G>
    where
        L: FnOnce() -> G,
    {
        match control.flow(pause_duration) {
            BackgroundWorkerFlow::Continue => {}
            BackgroundWorkerFlow::Pause(duration) => {
                return BackgroundWorkerPass::Pause(duration);
            }
            BackgroundWorkerFlow::Exit => return BackgroundWorkerPass::Exit,
        }

        let guard = acquire_guard();
        match control.flow(pause_duration) {
            BackgroundWorkerFlow::Continue => BackgroundWorkerPass::Ready(guard),
            BackgroundWorkerFlow::Pause(duration) => BackgroundWorkerPass::Pause(duration),
            BackgroundWorkerFlow::Exit => BackgroundWorkerPass::Exit,
        }
    }

    fn run_background_worker_pass<G, L, F>(
        control: &BackgroundWorkerControl<'_>,
        worker: &'static str,
        pause_duration: Duration,
        acquire_guard: L,
        run: F,
    ) -> BackgroundWorkerFlow
    where
        L: FnOnce() -> G,
        F: FnOnce() -> Result<()>,
    {
        match Self::begin_background_worker_pass(control, pause_duration, acquire_guard) {
            BackgroundWorkerPass::Ready(_guard) => control.handle_result(worker, run()),
            BackgroundWorkerPass::Pause(duration) => BackgroundWorkerFlow::Pause(duration),
            BackgroundWorkerPass::Exit => BackgroundWorkerFlow::Exit,
        }
    }

    fn needs_background_persisted_refresh_thread(&self) -> bool {
        self.persisted.numeric_lane_path.is_some()
            || self.persisted.blob_lane_path.is_some()
            || (self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly
                && self.persisted.tiered_storage.is_some())
    }

    fn background_persisted_refresh_poll_interval(&self) -> Duration {
        if self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly
            && self.persisted.tiered_storage.is_some()
        {
            return self
                .persisted
                .remote_segment_refresh_interval
                .min(DEFAULT_FLUSH_INTERVAL)
                .max(Duration::from_millis(1));
        }

        DEFAULT_FLUSH_INTERVAL
    }

    fn spawn_background_rollup_thread(
        storage: std::sync::Weak<Self>,
        rollup_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-rollups".to_string())
            .spawn(move || loop {
                std::thread::park_timeout(rollup_interval);

                let Some(storage) = storage.upgrade() else {
                    break;
                };

                let control = BackgroundWorkerControl::for_storage(storage.as_ref());
                match Self::run_background_worker_pass(
                    &control,
                    "rollup",
                    rollup_interval,
                    || storage.background_maintenance_gate(),
                    || {
                        // The rollup pipeline shares `rollup_run_lock` with policy mutations, so
                        // background runs only execute against a fully persisted policy/runtime snapshot.
                        storage.run_rollup_pipeline_once()
                    },
                ) {
                    BackgroundWorkerFlow::Continue | BackgroundWorkerFlow::Pause(_) => {}
                    BackgroundWorkerFlow::Exit => break,
                }
            })?;

        Ok(Some(handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_background_compaction_thread(
        lifecycle: std::sync::Weak<AtomicU8>,
        compaction_lock: Arc<Mutex<()>>,
        numeric_compactor: Option<Compactor>,
        blob_compactor: Option<Compactor>,
        persisted_index_dirty: Arc<AtomicBool>,
        pending_persisted_segment_diff: Arc<Mutex<PendingPersistedSegmentDiff>>,
        compaction_interval: Duration,
        observability: Arc<StorageObservabilityCounters>,
        background_fail_fast: bool,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if numeric_compactor.is_none() && blob_compactor.is_none() {
            return Ok(None);
        }

        let handle = std::thread::Builder::new()
            .name("tsink-compaction".to_string())
            .spawn(move || loop {
                std::thread::park_timeout(compaction_interval);

                let Some(lifecycle) = lifecycle.upgrade() else {
                    break;
                };

                let control = BackgroundWorkerControl::new(
                    lifecycle.as_ref(),
                    observability.as_ref(),
                    background_fail_fast,
                );
                match Self::run_background_worker_pass(
                    &control,
                    "compaction",
                    compaction_interval,
                    || Self::lock_compaction_gate(compaction_lock.as_ref()),
                    || {
                        match Self::compact_compactors_with_changes(
                            numeric_compactor.as_ref(),
                            blob_compactor.as_ref(),
                            Some(observability.as_ref()),
                        ) {
                            Ok(changes) if !changes.is_empty() => {
                                pending_persisted_segment_diff.lock().merge(changes);
                                persisted_index_dirty.store(true, Ordering::SeqCst);
                            }
                            Ok(_) => {}
                            Err(err) => return Err(err),
                        }
                        Ok(())
                    },
                ) {
                    BackgroundWorkerFlow::Continue | BackgroundWorkerFlow::Pause(_) => {}
                    BackgroundWorkerFlow::Exit => break,
                }
            })?;

        Ok(Some(handle))
    }

    fn spawn_background_flush_thread(
        storage: std::sync::Weak<Self>,
        flush_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-flush".to_string())
            .spawn(move || {
                let mut schedule = FlushWorkerSchedule::new(flush_interval);
                loop {
                    schedule.park_until_due();

                    let Some(storage) = storage.upgrade() else {
                        break;
                    };

                    let explicit_wakeup = storage.background.take_flush_wakeup_request();
                    let Some(policy) = schedule.next_policy(Instant::now(), explicit_wakeup) else {
                        continue;
                    };

                    let control = BackgroundWorkerControl::for_storage(storage.as_ref());
                    let flush_result = match policy {
                        FlushPipelinePolicy::BackgroundBounded => Self::run_background_worker_pass(
                            &control,
                            "flush",
                            flush_interval,
                            || storage.background_maintenance_gate(),
                            || storage.background_flush_pipeline_once(),
                        ),
                        FlushPipelinePolicy::BackgroundEligibleOnly => {
                            Self::run_background_worker_pass(
                                &control,
                                "flush",
                                flush_interval,
                                || storage.background_maintenance_gate(),
                                || storage.background_flush_eligible_pipeline_once(),
                            )
                        }
                        FlushPipelinePolicy::Foreground => unreachable!(),
                    };

                    match flush_result {
                        BackgroundWorkerFlow::Continue | BackgroundWorkerFlow::Pause(_) => {}
                        BackgroundWorkerFlow::Exit => break,
                    }
                }
            })?;

        Ok(Some(handle))
    }

    pub(super) fn start_background_flush_thread(
        self: &Arc<Self>,
        flush_interval: Duration,
    ) -> Result<()> {
        if self.persisted.numeric_lane_path.is_none() && self.persisted.blob_lane_path.is_none() {
            return Ok(());
        }

        self.background
            .install_thread(BackgroundThreadKind::Flush, || {
                Self::spawn_background_flush_thread(Arc::downgrade(self), flush_interval)
            })
    }

    fn spawn_background_persisted_refresh_thread(
        storage: std::sync::Weak<Self>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-persisted-refresh".to_string())
            .spawn(move || loop {
                let Some(storage) = storage.upgrade() else {
                    break;
                };

                let interval = storage.background_persisted_refresh_poll_interval();
                let control = BackgroundWorkerControl::for_storage(storage.as_ref());
                let maintenance_pass =
                    Self::begin_background_worker_pass(&control, interval, || {
                        storage.background_maintenance_gate()
                    });
                let _maintenance_guard = match maintenance_pass {
                    BackgroundWorkerPass::Ready(guard) => guard,
                    BackgroundWorkerPass::Pause(duration) => {
                        std::thread::park_timeout(duration);
                        continue;
                    }
                    BackgroundWorkerPass::Exit => break,
                };

                match control.handle_result(
                    "flush_maintenance",
                    storage.run_post_flush_maintenance_if_pending(),
                ) {
                    BackgroundWorkerFlow::Continue => {}
                    BackgroundWorkerFlow::Pause(duration) => {
                        drop(_maintenance_guard);
                        std::thread::park_timeout(duration);
                        continue;
                    }
                    BackgroundWorkerFlow::Exit => break,
                }

                match control.handle_result(
                    "persisted_refresh",
                    storage.sync_persisted_segments_from_disk_if_dirty(),
                ) {
                    BackgroundWorkerFlow::Continue => {}
                    BackgroundWorkerFlow::Pause(duration) => {
                        drop(_maintenance_guard);
                        std::thread::park_timeout(duration);
                        continue;
                    }
                    BackgroundWorkerFlow::Exit => break,
                }

                drop(_maintenance_guard);
                std::thread::park_timeout(interval);
            })?;

        Ok(Some(handle))
    }

    pub(super) fn start_background_persisted_refresh_thread(self: &Arc<Self>) -> Result<()> {
        if !self.needs_background_persisted_refresh_thread() {
            return Ok(());
        }

        self.background
            .install_thread(BackgroundThreadKind::PersistedRefresh, || {
                Self::spawn_background_persisted_refresh_thread(Arc::downgrade(self))
            })
    }

    pub(super) fn start_background_rollup_thread(
        self: &Arc<Self>,
        rollup_interval: Duration,
    ) -> Result<()> {
        if self.rollups.runtime.dir_path().is_none() {
            return Ok(());
        }

        self.background
            .install_thread(BackgroundThreadKind::Rollup, || {
                Self::spawn_background_rollup_thread(Arc::downgrade(self), rollup_interval)
            })
    }

    fn record_background_worker_error(
        worker: &'static str,
        error: &TsinkError,
        observability: &StorageObservabilityCounters,
        fail_fast_enabled: bool,
    ) {
        observability.record_background_worker_error(worker, error, fail_fast_enabled);
        tracing::error!(
            worker = worker,
            fail_fast_enabled,
            error = %error,
            "Background worker execution failed"
        );
    }

    pub(super) fn flush_pipeline_once(&self) -> Result<()> {
        self.flush_pipeline_once_with_policy(FlushPipelinePolicy::Foreground)
    }

    pub(super) fn background_flush_pipeline_once(&self) -> Result<()> {
        self.flush_pipeline_once_with_policy(FlushPipelinePolicy::BackgroundBounded)
    }

    fn background_flush_eligible_pipeline_once(&self) -> Result<()> {
        self.flush_pipeline_once_with_policy(FlushPipelinePolicy::BackgroundEligibleOnly)
    }

    fn flush_pipeline_once_with_policy(&self, policy: FlushPipelinePolicy) -> Result<()> {
        self.observability
            .flush
            .pipeline_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        if self.persisted.numeric_lane_path.is_none() && self.persisted.blob_lane_path.is_none() {
            self.observability
                .flush
                .pipeline_success_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Ok(());
        }

        let flush_result = match policy {
            FlushPipelinePolicy::Foreground => self.flush_all_active(),
            FlushPipelinePolicy::BackgroundEligibleOnly => self.flush_background_eligible_active(),
            FlushPipelinePolicy::BackgroundBounded => self.flush_background_bounded_active(),
        };
        if let Err(err) = flush_result {
            self.observability
                .flush
                .pipeline_errors_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }

        if let Err(err) = self.persist_segment_with_outcome() {
            self.observability
                .flush
                .pipeline_errors_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }
        if let Err(err) = self.schedule_post_flush_maintenance() {
            self.observability
                .flush
                .pipeline_errors_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }
        if self.persisted.persisted_index_dirty.load(Ordering::SeqCst) {
            self.notify_persisted_refresh_thread();
        }
        self.observability
            .flush
            .pipeline_success_total
            .fetch_add(1, Ordering::Relaxed);
        self.observability
            .flush
            .pipeline_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_worker_control_uses_shared_lifecycle_flow() {
        let pause = Duration::from_secs(3);

        assert_eq!(
            BackgroundWorkerControl::flow_for_state(STORAGE_OPEN, false, pause),
            BackgroundWorkerFlow::Continue
        );
        assert_eq!(
            BackgroundWorkerControl::flow_for_state(STORAGE_CLOSING, false, pause),
            BackgroundWorkerFlow::Pause(pause)
        );
        assert_eq!(
            BackgroundWorkerControl::flow_for_state(STORAGE_CLOSED, false, pause),
            BackgroundWorkerFlow::Exit
        );
        assert_eq!(
            BackgroundWorkerControl::flow_for_state(STORAGE_OPEN, true, pause),
            BackgroundWorkerFlow::Exit
        );
    }

    #[test]
    fn background_worker_pass_rechecks_lifecycle_after_guard_acquisition() {
        let lifecycle = AtomicU8::new(STORAGE_OPEN);
        let observability = StorageObservabilityCounters::default();
        let control = BackgroundWorkerControl::new(&lifecycle, &observability, false);

        let pass =
            ChunkStorage::begin_background_worker_pass(&control, Duration::from_secs(1), || {
                lifecycle.store(STORAGE_CLOSED, Ordering::SeqCst);
            });

        assert!(matches!(pass, BackgroundWorkerPass::Exit));
    }

    #[test]
    fn flush_worker_schedule_distinguishes_explicit_and_bounded_runs() {
        let interval = Duration::from_millis(10);
        let mut schedule = FlushWorkerSchedule::new(interval);
        let now = Instant::now();

        schedule.next_bounded_flush_at = now + interval;
        assert_eq!(
            schedule.next_policy(now, true),
            Some(FlushPipelinePolicy::BackgroundEligibleOnly)
        );
        assert_eq!(schedule.next_bounded_flush_at, now + interval);

        let bounded_at = now + interval;
        assert_eq!(
            schedule.next_policy(bounded_at, false),
            Some(FlushPipelinePolicy::BackgroundBounded)
        );
        assert_eq!(schedule.next_bounded_flush_at, bounded_at + interval);

        let before_deadline = bounded_at + Duration::from_millis(1);
        assert_eq!(schedule.next_policy(before_deadline, false), None);
    }
}
