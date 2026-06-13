use super::*;

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

    pub(super) fn install_thread(
        &self,
        worker: BackgroundThreadKind,
        spawn: impl FnOnce() -> Result<Option<std::thread::JoinHandle<()>>>,
    ) -> Result<()> {
        let mut thread = self.thread_slot(worker).lock();
        if thread.is_some() {
            return Ok(());
        }

        *thread = spawn()?;
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

    fn join_thread(handle: std::thread::JoinHandle<()>, worker_name: &str) -> Result<()> {
        if handle.thread().id() == std::thread::current().id() {
            return Ok(());
        }
        handle.join().map_err(|payload| {
            TsinkError::Other(format!(
                "{worker_name} worker panicked: {}",
                Self::panic_payload_message(payload)
            ))
        })
    }

    fn join_all_threads(&self) -> Result<()> {
        for worker in BackgroundThreadKind::SHUTDOWN_ORDER {
            if let Some(thread) = self.take_thread(worker) {
                Self::join_thread(thread, worker.worker_name())?;
            }
        }
        Ok(())
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
            self.release_data_path_process_lock();
            if let Err(err) = self.join_background_threads() {
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_background_supervisor() -> BackgroundWorkerSupervisorState {
        BackgroundWorkerSupervisorState {
            compaction_thread: Mutex::new(None),
            flush_thread: Mutex::new(None),
            flush_thread_wakeup_requested: AtomicBool::new(false),
            persisted_refresh_thread: Mutex::new(None),
            rollup_thread: Mutex::new(None),
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
    fn background_worker_slots_install_only_once() {
        let supervisor = test_background_supervisor();
        let spawn_count = AtomicUsize::new(0);

        supervisor
            .install_thread(BackgroundThreadKind::Flush, || {
                spawn_count.fetch_add(1, Ordering::SeqCst);
                Ok(Some(std::thread::spawn(|| {})))
            })
            .unwrap();
        supervisor
            .install_thread(BackgroundThreadKind::Flush, || {
                spawn_count.fetch_add(1, Ordering::SeqCst);
                Ok(Some(std::thread::spawn(|| {})))
            })
            .unwrap();

        assert_eq!(spawn_count.load(Ordering::SeqCst), 1);
        supervisor.join_all_threads().unwrap();
    }

    #[test]
    fn join_errors_include_worker_name() {
        let supervisor = test_background_supervisor();

        supervisor
            .install_thread(BackgroundThreadKind::Compaction, || {
                Ok(Some(std::thread::spawn(|| panic!("boom"))))
            })
            .unwrap();

        let err = supervisor.join_all_threads().unwrap_err();
        let TsinkError::Other(message) = err else {
            panic!("expected panic join error");
        };
        assert!(message.contains("compaction worker panicked"));
        assert!(message.contains("boom"));
    }
}
