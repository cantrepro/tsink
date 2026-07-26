use super::*;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn build_visibility_state() -> VisibilityState {
        VisibilityState {
            tombstones: Arc::new(RwLock::new(TombstoneMap::new())),
            remote_tombstones: RwLock::new(tombstone::ImmutableTombstoneSnapshot::empty()),
            materialized_series: RwLock::new(BTreeSet::new()),
            series_visibility_summaries: RwLock::new(HashMap::new()),
            series_visible_max_timestamps: RwLock::new(HashMap::new()),
            series_visible_bounded_max_timestamps: RwLock::new(HashMap::new()),
            series_visibility_cache_epochs: RwLock::new(HashMap::new()),
            #[cfg(test)]
            visibility_cache_accounting_entries_visited: AtomicU64::new(0),
            remote_tombstone_epoch: AtomicU64::new(0),
            visibility_state_generation: AtomicU64::new(0),
            tombstone_state_generation: AtomicU64::new(0),
            live_series_pruning_generation: AtomicU64::new(0),
            max_observed_timestamp: AtomicI64::new(i64::MIN),
            max_bounded_observed_timestamp: AtomicI64::new(i64::MIN),
            recency_state_lock: Mutex::new(()),
            flush_visibility_lock: RwLock::new(()),
        }
    }

    pub(in crate::engine::storage_engine) fn visibility_state_generation(&self) -> u64 {
        self.visibility
            .visibility_state_generation
            .load(Ordering::Acquire)
    }

    pub(in crate::engine::storage_engine) fn bump_visibility_state_generation(&self) {
        self.visibility
            .visibility_state_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::engine::storage_engine) fn tombstone_state_generation(&self) -> u64 {
        self.visibility
            .tombstone_state_generation
            .load(Ordering::Acquire)
    }

    pub(in crate::engine::storage_engine) fn bump_tombstone_state_generation(&self) {
        self.visibility
            .tombstone_state_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::engine::storage_engine) fn remote_tombstone_epoch(&self) -> u64 {
        self.visibility
            .remote_tombstone_epoch
            .load(Ordering::Acquire)
    }

    pub(in crate::engine::storage_engine) fn visibility_read_fence(
        &self,
    ) -> RwLockReadGuard<'_, ()> {
        self.visibility.flush_visibility_lock.read()
    }

    pub(in crate::engine::storage_engine) fn visibility_write_fence(
        &self,
    ) -> RwLockWriteGuard<'_, ()> {
        self.visibility.flush_visibility_lock.write()
    }

    pub(in crate::engine::storage_engine) fn with_visibility_write_stage<R>(
        &self,
        f: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        let _visibility_guard = self.visibility_write_fence();
        f()
    }

    fn update_atomic_max_timestamp(target: &AtomicI64, ts: i64) {
        let mut current = target.load(Ordering::Acquire);
        while ts > current {
            match target.compare_exchange_weak(current, ts, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    pub(in crate::engine::storage_engine) fn update_max_observed_timestamp(&self, ts: i64) {
        Self::update_atomic_max_timestamp(&self.visibility.max_observed_timestamp, ts);
    }

    pub(in crate::engine::storage_engine) fn max_observed_timestamp(&self) -> Option<i64> {
        let max_observed = self
            .visibility
            .max_observed_timestamp
            .load(Ordering::Acquire);
        (max_observed != i64::MIN).then_some(max_observed)
    }

    pub(in crate::engine::storage_engine) fn bounded_recency_reference_timestamp(
        &self,
    ) -> Option<i64> {
        let max_bounded = self
            .visibility
            .max_bounded_observed_timestamp
            .load(Ordering::Acquire);
        (max_bounded != i64::MIN).then_some(max_bounded)
    }

    pub(in crate::engine::storage_engine) fn retention_recency_reference_timestamp(
        &self,
    ) -> Option<i64> {
        let max_observed = self
            .visibility
            .max_observed_timestamp
            .load(Ordering::Acquire);
        let bounded_reference = self.bounded_recency_reference_timestamp();
        if max_observed == i64::MIN && bounded_reference.is_none() {
            return None;
        }

        Some(
            bounded_reference
                .map(|reference| reference.max(self.current_timestamp_units()))
                .unwrap_or_else(|| self.current_timestamp_units()),
        )
    }
}
