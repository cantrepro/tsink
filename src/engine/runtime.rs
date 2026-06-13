use super::*;

impl ChunkStorage {
    pub(super) fn spawn_background_compaction_thread(
        lifecycle: std::sync::Weak<AtomicU8>,
        compaction_lock: Arc<Mutex<()>>,
        numeric_compactor: Option<Compactor>,
        blob_compactor: Option<Compactor>,
        compaction_interval: Duration,
        observability: Arc<StorageObservabilityCounters>,
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

                match lifecycle.load(Ordering::SeqCst) {
                    STORAGE_OPEN => {}
                    STORAGE_CLOSED => break,
                    _ => continue,
                }

                let _compaction_guard = compaction_lock.lock();
                let _ = Self::compact_compactors(
                    numeric_compactor.as_ref(),
                    blob_compactor.as_ref(),
                    Some(observability.as_ref()),
                );
            })?;

        Ok(Some(handle))
    }

    pub(super) fn notify_compaction_thread(&self) {
        if let Some(compaction_thread) = self.compaction_thread.lock().as_ref() {
            compaction_thread.thread().unpark();
        }
    }

    fn spawn_background_flush_thread(
        storage: std::sync::Weak<Self>,
        flush_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-flush".to_string())
            .spawn(move || loop {
                std::thread::park_timeout(flush_interval);

                let Some(storage) = storage.upgrade() else {
                    break;
                };

                match storage.lifecycle.load(Ordering::SeqCst) {
                    STORAGE_OPEN => {}
                    STORAGE_CLOSED => break,
                    _ => continue,
                }

                let _ = storage.flush_pipeline_once();
            })?;

        Ok(Some(handle))
    }

    pub(super) fn start_background_flush_thread(
        self: &Arc<Self>,
        flush_interval: Duration,
    ) -> Result<()> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let mut flush_thread = self.flush_thread.lock();
        if flush_thread.is_some() {
            return Ok(());
        }

        *flush_thread = Self::spawn_background_flush_thread(Arc::downgrade(self), flush_interval)?;
        Ok(())
    }

    pub(super) fn notify_flush_thread(&self) {
        if let Some(flush_thread) = self.flush_thread.lock().as_ref() {
            flush_thread.thread().unpark();
        }
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

    fn join_background_thread(
        handle: std::thread::JoinHandle<()>,
        worker_name: &str,
    ) -> Result<()> {
        handle.join().map_err(|payload| {
            TsinkError::Other(format!(
                "{worker_name} worker panicked: {}",
                Self::panic_payload_message(payload)
            ))
        })
    }

    pub(super) fn join_background_threads(&self) -> Result<()> {
        if let Some(compaction_thread) = self.compaction_thread.lock().take() {
            Self::join_background_thread(compaction_thread, "compaction")?;
        }
        if let Some(flush_thread) = self.flush_thread.lock().take() {
            Self::join_background_thread(flush_thread, "flush")?;
        }
        Ok(())
    }

    pub(super) fn flush_pipeline_once(&self) -> Result<()> {
        self.observability
            .flush
            .pipeline_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
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

        // Drain writer permits with a bounded wait so background flush can still make progress
        // under sustained write load instead of bailing immediately when one permit is busy.
        let write_permits = match self.write_limiter.acquire_all(self.write_timeout) {
            Ok(permits) => permits,
            Err(TsinkError::WriteTimeout { .. }) => {
                self.observability
                    .flush
                    .pipeline_timeout_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .pipeline_duration_nanos_total
                    .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
                return Ok(());
            }
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
        let flush_result = self.flush_all_active();
        if let Err(err) = flush_result {
            drop(write_permits);
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

        let persisted = match self.persist_segment(true) {
            Ok(persisted) => persisted,
            Err(err) => {
                drop(write_permits);
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
        drop(write_permits);
        if persisted {
            if let Err(err) = self.refresh_persisted_indexes_and_evict_flushed_sealed_chunks() {
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
        }
        if let Err(err) = self.sweep_expired_persisted_segments() {
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
