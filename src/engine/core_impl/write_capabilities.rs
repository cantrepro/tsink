use super::*;

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WorkerWakeContext<'a> {
    pub(in crate::engine::storage_engine) background: &'a BackgroundWorkerSupervisorState,
}

impl<'a> WorkerWakeContext<'a> {
    fn wake_thread(
        thread_slot: &Mutex<Option<std::thread::JoinHandle<()>>>,
        wakeup_requested: Option<&AtomicBool>,
    ) {
        if let Some(wakeup_requested) = wakeup_requested {
            wakeup_requested.store(true, Ordering::SeqCst);
        }
        let thread_slot = thread_slot.lock();
        if let Some(thread) = thread_slot.as_ref() {
            thread.thread().unpark();
        }
    }

    pub(in crate::engine::storage_engine) fn notify_flush_thread(self) {
        let background = self.background;
        Self::wake_thread(
            &background.flush_thread,
            Some(&background.flush_thread_wakeup_requested),
        );
    }

    pub(in crate::engine::storage_engine) fn notify_persisted_refresh_thread(self) {
        Self::wake_thread(&self.background.persisted_refresh_thread, None);
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WalMetricsContext<'a> {
    pub(in crate::engine::storage_engine) metrics:
        &'a super::super::metrics::WalObservabilityCounters,
}

impl<'a> WalMetricsContext<'a> {
    pub(in crate::engine::storage_engine) fn record_append_error(self) {
        self.metrics
            .append_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(in crate::engine::storage_engine) fn record_committed_append(
        self,
        series_definitions: usize,
        sample_batches: usize,
        sample_points: usize,
        encoded_bytes: u64,
    ) {
        let metrics = self.metrics;
        metrics.append_series_definitions_total.fetch_add(
            saturating_u64_from_usize(series_definitions),
            Ordering::Relaxed,
        );
        metrics
            .append_sample_batches_total
            .fetch_add(saturating_u64_from_usize(sample_batches), Ordering::Relaxed);
        metrics
            .append_points_total
            .fetch_add(saturating_u64_from_usize(sample_points), Ordering::Relaxed);
        metrics
            .append_bytes_total
            .fetch_add(encoded_bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyTestHooksContext<'a> {
    pub(in crate::engine::storage_engine) hooks: &'a PersistTestHooks,
}

#[cfg(test)]
impl<'a> WriteApplyTestHooksContext<'a> {
    pub(in crate::engine::storage_engine) fn invoke_post_samples_append_hook(self) {
        if let Some(hook) = self.hooks.post_samples_append_hook.read().clone() {
            hook();
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteCommitTestHooksContext<'a> {
    pub(in crate::engine::storage_engine) hooks: &'a PersistTestHooks,
}

#[cfg(test)]
impl<'a> WriteCommitTestHooksContext<'a> {
    pub(in crate::engine::storage_engine) fn invoke_post_series_definitions_append_hook(self) {
        if let Some(hook) = self
            .hooks
            .post_series_definitions_append_hook
            .read()
            .clone()
        {
            hook();
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteResolveSupportContext<'a> {
    pub(in crate::engine::storage_engine) registry_memory: RegistryMemoryContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WritePrepareConfigContext {
    pub(in crate::engine::storage_engine) chunk_point_cap: usize,
    pub(in crate::engine::storage_engine) partition_window: i64,
    pub(in crate::engine::storage_engine) max_active_partition_heads_per_series: usize,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WritePrepareVisibilityContext<'a> {
    pub(in crate::engine::storage_engine) clock: ClockContext<'a>,
    pub(in crate::engine::storage_engine) pending_series_ids: &'a RwLock<BTreeSet<SeriesId>>,
    pub(in crate::engine::storage_engine) has_metadata_shards: bool,
    pub(in crate::engine::storage_engine) materialized_series: MaterializedSeriesReadContext<'a>,
    pub(in crate::engine::storage_engine) visibility_cache: VisibilityCacheReadContext<'a>,
    pub(in crate::engine::storage_engine) max_bounded_observed_timestamp: &'a AtomicI64,
    pub(in crate::engine::storage_engine) retention_window: i64,
    pub(in crate::engine::storage_engine) future_skew_window: i64,
    pub(in crate::engine::storage_engine) retention_enforced: bool,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WritePrepareWalContext<'a> {
    pub(in crate::engine::storage_engine) wal: Option<&'a FramedWal>,
    pub(in crate::engine::storage_engine) wal_size_limit_bytes: u64,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WritePrepareMemoryBudgetContext<'a> {
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) budget_bytes: &'a AtomicU64,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteAdmissionControlContext<'a> {
    pub(in crate::engine::storage_engine) budget: PersistedSealedBudgetContext<'a>,
    pub(in crate::engine::storage_engine) workers: WorkerWakeContext<'a>,
    pub(in crate::engine::storage_engine) observability: &'a StorageObservabilityCounters,
    pub(in crate::engine::storage_engine) write_timeout: Duration,
    pub(in crate::engine::storage_engine) admission_poll_interval: Duration,
    pub(in crate::engine::storage_engine) admission_backpressure_lock: &'a Mutex<()>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyRegistryContext<'a> {
    pub(in crate::engine::storage_engine) series_validation: WriteSeriesValidationContext<'a>,
    pub(in crate::engine::storage_engine) registry_memory: RegistryMemoryContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyWalContext<'a> {
    #[cfg(test)]
    pub(in crate::engine::storage_engine) test_hooks: WriteApplyTestHooksContext<'a>,
    #[cfg(test)]
    pub(in crate::engine::storage_engine) crash_after_samples_persisted: &'a AtomicBool,
    #[cfg(not(test))]
    pub(in crate::engine::storage_engine) marker: std::marker::PhantomData<&'a ()>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyShardMutationContext<'a> {
    pub(in crate::engine::storage_engine) chunks: ChunkContext<'a>,
    pub(in crate::engine::storage_engine) timestamps: SeriesTimestampWriteContext<'a>,
    pub(in crate::engine::storage_engine) chunk_point_cap: usize,
    pub(in crate::engine::storage_engine) partition_window: i64,
    pub(in crate::engine::storage_engine) max_active_partition_heads_per_series: usize,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyMemoryAccountingContext<'a> {
    pub(in crate::engine::storage_engine) shards: ShardMemoryAccountingContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyPublicationContext<'a> {
    pub(in crate::engine::storage_engine) registry_bookkeeping: RegistryBookkeepingContext<'a>,
    pub(in crate::engine::storage_engine) materialized_series: MaterializedSeriesWriteContext<'a>,
    pub(in crate::engine::storage_engine) runtime_metadata_delta:
        RuntimeMetadataDeltaWriteContext<'a>,
    pub(in crate::engine::storage_engine) metadata_shards: MetadataShardPublicationContext<'a>,
    pub(in crate::engine::storage_engine) sealed_chunks: SealedChunkPublishContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteCommitStageContext<'a> {
    pub(in crate::engine::storage_engine) chunks: ChunkContext<'a>,
    pub(in crate::engine::storage_engine) wal: Option<&'a FramedWal>,
    pub(in crate::engine::storage_engine) wal_metrics: WalMetricsContext<'a>,
    #[cfg(test)]
    pub(in crate::engine::storage_engine) test_hooks: WriteCommitTestHooksContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteCommitWalCompletionContext<'a> {
    pub(in crate::engine::storage_engine) observability: &'a StorageObservabilityCounters,
    pub(in crate::engine::storage_engine) wal: Option<&'a FramedWal>,
    pub(in crate::engine::storage_engine) wal_metrics: WalMetricsContext<'a>,
    #[cfg(test)]
    pub(in crate::engine::storage_engine) crash_before_publish_persisted: &'a AtomicBool,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct LifecyclePublicationContext<'a> {
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
    pub(in crate::engine::storage_engine) persisted_index: &'a RwLock<PersistedIndexState>,
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) registry_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) persisted_index_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) persisted_mmap_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) registry_bookkeeping: RegistryBookkeepingContext<'a>,
    pub(in crate::engine::storage_engine) materialized_series: MaterializedSeriesWriteContext<'a>,
    pub(in crate::engine::storage_engine) runtime_metadata_delta:
        RuntimeMetadataDeltaWriteContext<'a>,
    pub(in crate::engine::storage_engine) metadata_shards: MetadataShardPublicationContext<'a>,
}
