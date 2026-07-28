use super::{
    elapsed_nanos_u64, Arc, AtomicBool, AtomicU8, BackgroundWorkerRuntimeState,
    BackgroundWorkerSupervisorState, ChunkStorage, Compactor, Duration, Instant,
    MaintenancePassSelection, Mutex, Ordering, PendingPersistedSegmentDiff, Result,
    StorageObservabilityCounters, StorageRuntimeMode, TsinkError, DEFAULT_FLUSH_INTERVAL,
    STORAGE_CLOSED, STORAGE_CLOSING, STORAGE_OPEN,
};
use crate::engine::tombstone::TombstoneMap;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};

#[path = "runtime/supervision.rs"]
mod supervision;

use self::supervision::{
    BackgroundThreadKind, BackgroundWorkerPassGuard, BackgroundWorkerRunGuard,
};

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

    fn park_until_due(&self, runtime: &BackgroundWorkerRuntimeState) {
        let now = Instant::now();
        if now < self.next_bounded_flush_at {
            runtime.record_idle_wait();
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
        runtime: &BackgroundWorkerRuntimeState,
        pause_duration: Duration,
        acquire_guard: L,
        run: F,
    ) -> BackgroundWorkerFlow
    where
        L: FnOnce() -> G,
        F: FnOnce() -> Result<()>,
    {
        match Self::begin_background_worker_pass(control, pause_duration, acquire_guard) {
            BackgroundWorkerPass::Ready(_guard) => {
                let _pass = BackgroundWorkerPassGuard::new(runtime);
                control.handle_result(worker, run())
            }
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
        runtime: Arc<BackgroundWorkerRuntimeState>,
        rollup_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-rollups".to_string())
            .spawn(move || {
                let _run = BackgroundWorkerRunGuard::new(Arc::clone(&runtime));
                loop {
                    runtime.record_idle_wait();
                    std::thread::park_timeout(rollup_interval);

                    let Some(storage) = storage.upgrade() else {
                        break;
                    };

                    let control = BackgroundWorkerControl::for_storage(storage.as_ref());
                    match Self::run_background_worker_pass(
                        &control,
                        "rollup",
                        runtime.as_ref(),
                        rollup_interval,
                        || storage.background_maintenance_gate(),
                        || {
                            // The rollup pipeline shares `rollup_run_lock` with policy mutations, so
                            // background runs only execute against a fully persisted policy/runtime snapshot.
                            storage.run_shared_background_rollup_pipeline_once()
                        },
                    ) {
                        BackgroundWorkerFlow::Continue | BackgroundWorkerFlow::Pause(_) => {}
                        BackgroundWorkerFlow::Exit => break,
                    }
                }
            })?;

        Ok(Some(handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_background_compaction_thread(
        lifecycle: std::sync::Weak<AtomicU8>,
        compaction_lock: Arc<Mutex<()>>,
        post_flush_replacement_data_path: Option<PathBuf>,
        numeric_compactor: Option<Compactor>,
        blob_compactor: Option<Compactor>,
        tombstones: Arc<RwLock<TombstoneMap>>,
        persisted_index_dirty: Arc<AtomicBool>,
        pending_persisted_segment_diff: Arc<Mutex<PendingPersistedSegmentDiff>>,
        runtime: Arc<BackgroundWorkerRuntimeState>,
        compaction_interval: Duration,
        observability: Arc<StorageObservabilityCounters>,
        background_fail_fast: bool,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if numeric_compactor.is_none() && blob_compactor.is_none() {
            return Ok(None);
        }

        let handle = std::thread::Builder::new()
            .name("tsink-compaction".to_string())
            .spawn(move || {
                let _run = BackgroundWorkerRunGuard::new(Arc::clone(&runtime));
                let mut prefer_blob = false;
                loop {
                    runtime.record_idle_wait();
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
                        runtime.as_ref(),
                        compaction_interval,
                        || Self::lock_compaction_gate(compaction_lock.as_ref()),
                        || {
                            Self::compact_next_background_compactor_with_changes(
                                post_flush_replacement_data_path.as_deref(),
                                numeric_compactor.as_ref(),
                                blob_compactor.as_ref(),
                                prefer_blob,
                                Some(tombstones.as_ref()),
                                Some(observability.as_ref()),
                                |changes| {
                                    pending_persisted_segment_diff.lock().merge(changes);
                                    persisted_index_dirty.store(true, Ordering::SeqCst);
                                },
                            )?;
                            prefer_blob = !prefer_blob;
                            Ok(())
                        },
                    ) {
                        BackgroundWorkerFlow::Continue | BackgroundWorkerFlow::Pause(_) => {}
                        BackgroundWorkerFlow::Exit => break,
                    }
                }
            })?;

        Ok(Some(handle))
    }

    pub(super) fn start_background_compaction_thread(&self) -> Result<()> {
        if self.persisted.numeric_compactor.is_none() && self.persisted.blob_compactor.is_none() {
            return Ok(());
        }

        self.background.install_thread(
            BackgroundThreadKind::Compaction,
            self.background.compaction_interval,
            |runtime, interval| {
                Self::spawn_background_compaction_thread(
                    Arc::downgrade(&self.coordination.lifecycle),
                    Arc::clone(&self.coordination.compaction_lock),
                    self.persisted
                        .series_index_path
                        .as_deref()
                        .and_then(Path::parent)
                        .map(Path::to_path_buf),
                    self.persisted.numeric_compactor.clone(),
                    self.persisted.blob_compactor.clone(),
                    Arc::clone(&self.visibility.tombstones),
                    Arc::clone(&self.persisted.persisted_index_dirty),
                    Arc::clone(&self.persisted.pending_persisted_segment_diff),
                    runtime,
                    interval,
                    Arc::clone(&self.observability),
                    self.background.fail_fast_enabled,
                )
            },
        )
    }

    fn spawn_background_flush_thread(
        storage: std::sync::Weak<Self>,
        runtime: Arc<BackgroundWorkerRuntimeState>,
        flush_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-flush".to_string())
            .spawn(move || {
                let _run = BackgroundWorkerRunGuard::new(Arc::clone(&runtime));
                let mut schedule = FlushWorkerSchedule::new(flush_interval);
                loop {
                    schedule.park_until_due(runtime.as_ref());

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
                            runtime.as_ref(),
                            flush_interval,
                            || storage.background_maintenance_gate(),
                            || storage.background_flush_pipeline_once(),
                        ),
                        FlushPipelinePolicy::BackgroundEligibleOnly => {
                            Self::run_background_worker_pass(
                                &control,
                                "flush",
                                runtime.as_ref(),
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

        self.background.install_thread(
            BackgroundThreadKind::Flush,
            flush_interval,
            |runtime, interval| {
                Self::spawn_background_flush_thread(Arc::downgrade(self), runtime, interval)
            },
        )
    }

    fn spawn_background_persisted_refresh_thread(
        storage: std::sync::Weak<Self>,
        runtime: Arc<BackgroundWorkerRuntimeState>,
        configured_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-persisted-refresh".to_string())
            .spawn(move || {
                let _run = BackgroundWorkerRunGuard::new(Arc::clone(&runtime));
                loop {
                    let Some(storage) = storage.upgrade() else {
                        break;
                    };

                    let interval = configured_interval;
                    let park_duration = 'pass: {
                        let control = BackgroundWorkerControl::for_storage(storage.as_ref());
                        let maintenance_pass =
                            Self::begin_background_worker_pass(&control, interval, || {
                                storage.background_maintenance_gate()
                            });
                        let maintenance_guard = match maintenance_pass {
                            BackgroundWorkerPass::Ready(guard) => guard,
                            BackgroundWorkerPass::Pause(duration) => break 'pass Some(duration),
                            BackgroundWorkerPass::Exit => break 'pass None,
                        };
                        let _pass = BackgroundWorkerPassGuard::new(runtime.as_ref());

                        // When no higher-priority persisted delta is pending, recovery-snapshot
                        // reconciliation owns this wake's complete shared maintenance envelope.
                        // A stranded page therefore cannot starve post-flush/catalog publication,
                        // and one worker wake cannot multiply the configured item/byte limits.
                        if storage.bounded_tombstone_recovery_snapshot_is_pending()
                            && !storage
                                .coordination
                                .post_flush_maintenance_pending
                                .load(Ordering::Acquire)
                            && !storage
                                .persisted
                                .persisted_index_dirty
                                .load(Ordering::Acquire)
                            && !storage.has_known_persisted_segment_changes()
                        {
                            let flow = control.handle_result(
                                "tombstone_recovery_snapshot",
                                storage.run_bounded_tombstone_recovery_snapshot_if_pending(),
                            );
                            drop(maintenance_guard);
                            break 'pass match flow {
                                BackgroundWorkerFlow::Continue => Some(interval),
                                BackgroundWorkerFlow::Pause(duration) => Some(duration),
                                BackgroundWorkerFlow::Exit => None,
                            };
                        }

                        let post_flush_envelope =
                            match storage.run_post_flush_maintenance_envelope_if_pending() {
                                Ok(envelope_consumed) => envelope_consumed,
                                Err(err) => {
                                    let flow =
                                        control.handle_result::<()>("flush_maintenance", Err(err));
                                    drop(maintenance_guard);
                                    break 'pass match flow {
                                        BackgroundWorkerFlow::Continue => Some(interval),
                                        BackgroundWorkerFlow::Pause(duration) => Some(duration),
                                        BackgroundWorkerFlow::Exit => None,
                                    };
                                }
                            };
                        if post_flush_envelope {
                            // Retention and metadata pages each own the full configured
                            // maintenance envelope. End this wake instead of immediately
                            // dispatching a fresh persisted-catalog page under the same guard.
                            drop(maintenance_guard);
                            break 'pass Some(interval);
                        }

                        let park_duration = match control.handle_result(
                            "persisted_refresh",
                            storage.sync_persisted_segments_from_disk_if_dirty(),
                        ) {
                            BackgroundWorkerFlow::Continue => Some(interval),
                            BackgroundWorkerFlow::Pause(duration) => Some(duration),
                            BackgroundWorkerFlow::Exit => None,
                        };
                        drop(maintenance_guard);
                        park_duration
                    };

                    drop(storage);
                    let Some(park_duration) = park_duration else {
                        break;
                    };
                    runtime.record_idle_wait();
                    std::thread::park_timeout(park_duration);
                }
            })?;

        Ok(Some(handle))
    }

    pub(super) fn start_background_persisted_refresh_thread(self: &Arc<Self>) -> Result<()> {
        if !self.needs_background_persisted_refresh_thread() {
            return Ok(());
        }

        self.background.install_thread(
            BackgroundThreadKind::PersistedRefresh,
            self.background_persisted_refresh_poll_interval(),
            |runtime, interval| {
                Self::spawn_background_persisted_refresh_thread(
                    Arc::downgrade(self),
                    runtime,
                    interval,
                )
            },
        )
    }

    pub(super) fn start_background_rollup_thread(
        self: &Arc<Self>,
        rollup_interval: Duration,
    ) -> Result<()> {
        if self.rollups.runtime.dir_path().is_none() {
            return Ok(());
        }

        self.background.install_thread(
            BackgroundThreadKind::Rollup,
            rollup_interval,
            |runtime, interval| {
                Self::spawn_background_rollup_thread(Arc::downgrade(self), runtime, interval)
            },
        )
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
            FlushPipelinePolicy::Foreground => self
                .flush_all_active()
                .map(|()| MaintenancePassSelection::default()),
            FlushPipelinePolicy::BackgroundEligibleOnly => {
                self.flush_background_eligible_active_with_selection()
            }
            FlushPipelinePolicy::BackgroundBounded => {
                self.flush_background_bounded_active_with_selection()
            }
        };
        let active_selection = match flush_result {
            Ok(selection) => selection,
            Err(err) => {
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
        };

        let persist_result = match policy {
            FlushPipelinePolicy::BackgroundEligibleOnly
            | FlushPipelinePolicy::BackgroundBounded => self
                .persist_segment_background_bounded_with_limits(
                    self.runtime
                        .maintenance_max_items_per_pass
                        .saturating_sub(active_selection.inspected_items),
                    self.runtime
                        .maintenance_max_bytes_per_pass
                        .saturating_sub(active_selection.input_bytes),
                ),
            FlushPipelinePolicy::Foreground => self.persist_segment_with_outcome(),
        };
        if let Err(err) = persist_result {
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
