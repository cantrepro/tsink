use super::*;

#[path = "ingest.rs"]
mod ingest;
#[path = "query_exec.rs"]
mod query_exec;
#[path = "snapshot.rs"]
mod snapshot;

type PersistPostPublishHook = dyn Fn(&[PathBuf]) + Send + Sync + 'static;
pub(super) type IngestCommitHook = dyn Fn() + Send + Sync + 'static;

#[derive(Default)]
pub(super) struct PersistTestHooks {
    pub(super) post_publish_hook: RwLock<Option<Arc<PersistPostPublishHook>>>,
    pub(super) post_series_definitions_append_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) post_samples_append_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) pre_sealed_chunk_publish_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) crash_after_samples_persisted: AtomicBool,
    pub(super) crash_before_publish_persisted: AtomicBool,
    pub(super) full_inventory_scan_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) post_flush_maintenance_stage_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_all_series_seed_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_all_series_postings_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_persisted_postings_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_direct_candidate_scan_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_live_series_snapshot_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_live_series_pre_prune_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_time_range_summary_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_time_range_segment_prune_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_time_range_segment_spill_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) metadata_time_range_persisted_exact_scan_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) shard_metadata_fallback_scan_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) snapshot_pre_wal_copy_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) query_merge_in_memory_source_snapshot_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) query_append_sort_in_memory_source_snapshot_hook:
        RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) query_persisted_chunk_decode_hook: RwLock<Option<Arc<IngestCommitHook>>>,
    pub(super) tombstone_post_swap_pre_visibility_hook: RwLock<Option<Arc<IngestCommitHook>>>,
}

impl std::fmt::Debug for PersistTestHooks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PersistTestHooks").finish()
    }
}

fn invoke_commit_hook(slot: &RwLock<Option<Arc<IngestCommitHook>>>) {
    let hook = slot.read().clone();
    if let Some(hook) = hook {
        hook();
    }
}

fn set_commit_hook<F>(slot: &RwLock<Option<Arc<IngestCommitHook>>>, hook: F)
where
    F: Fn() + Send + Sync + 'static,
{
    *slot.write() = Some(Arc::new(hook));
}

fn clear_commit_hook(slot: &RwLock<Option<Arc<IngestCommitHook>>>) {
    slot.write().take();
}

fn set_crash_flag(flag: &AtomicBool) {
    flag.store(true, Ordering::SeqCst);
}
