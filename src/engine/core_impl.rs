use super::*;

impl ChunkStorage {
    pub fn new(chunk_point_cap: usize, wal: Option<FramedWal>) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            None,
            None,
            1,
            ChunkStorageOptions::default(),
        )
        .expect("failed to initialize chunk storage")
    }

    pub fn new_with_data_path(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
    ) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            numeric_lane_path,
            blob_lane_path,
            next_segment_id,
            ChunkStorageOptions::default(),
        )
        .expect("failed to initialize chunk storage")
    }

    pub(super) fn new_with_data_path_and_options(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
        options: ChunkStorageOptions,
    ) -> Result<Self> {
        let series_index_path = numeric_lane_path
            .as_ref()
            .and_then(|path| {
                path.parent()
                    .map(|parent| parent.join(SERIES_INDEX_FILE_NAME))
            })
            .or_else(|| {
                blob_lane_path.as_ref().and_then(|path| {
                    path.parent()
                        .map(|parent| parent.join(SERIES_INDEX_FILE_NAME))
                })
            });
        let next_segment_id = Arc::new(AtomicU64::new(next_segment_id.max(1)));
        let numeric_compactor = numeric_lane_path.as_ref().map(|path| {
            Compactor::new_with_segment_id_allocator(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
            )
        });
        let blob_compactor = blob_lane_path.as_ref().map(|path| {
            Compactor::new_with_segment_id_allocator(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
            )
        });
        let lifecycle = Arc::new(AtomicU8::new(STORAGE_OPEN));
        let compaction_lock = Arc::new(Mutex::new(()));
        let observability = Arc::new(StorageObservabilityCounters::default());
        let compaction_thread = if options.background_threads_enabled {
            Self::spawn_background_compaction_thread(
                Arc::downgrade(&lifecycle),
                Arc::clone(&compaction_lock),
                numeric_compactor.clone(),
                blob_compactor.clone(),
                options.compaction_interval,
                Arc::clone(&observability),
            )
        } else {
            Ok(None)
        };

        Ok(Self {
            registry: RwLock::new(SeriesRegistry::new()),
            materialized_series: RwLock::new(BTreeSet::new()),
            registry_write_txn_shards: std::array::from_fn(|_| Mutex::new(())),
            active_builders: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            sealed_chunks: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            persisted_index: RwLock::new(PersistedIndexState::default()),
            persisted_chunk_watermarks: RwLock::new(HashMap::new()),
            next_chunk_sequence: AtomicU64::new(1),
            chunk_point_cap: chunk_point_cap.clamp(1, u16::MAX as usize),
            numeric_compactor,
            blob_compactor,
            numeric_lane_path,
            blob_lane_path,
            series_index_path,
            next_segment_id,
            wal,
            retention_window: options.retention_window.max(0),
            retention_enforced: options.retention_enforced,
            partition_window: options.partition_window.max(1),
            write_limiter: Semaphore::new(options.max_writers.max(1)),
            write_timeout: options.write_timeout,
            memory_accounting_enabled: options.memory_budget_bytes != u64::MAX,
            memory_used_bytes: AtomicU64::new(0),
            memory_used_bytes_by_shard: std::array::from_fn(|_| AtomicU64::new(0)),
            memory_budget_bytes: AtomicU64::new(options.memory_budget_bytes),
            cardinality_limit: options.cardinality_limit,
            wal_size_limit_bytes: options.wal_size_limit_bytes,
            admission_poll_interval: options.admission_poll_interval,
            memory_backpressure_lock: Mutex::new(()),
            admission_backpressure_lock: Mutex::new(()),
            max_observed_timestamp: AtomicI64::new(i64::MIN),
            lifecycle,
            compaction_lock,
            flush_visibility_lock: RwLock::new(()),
            compaction_thread: Mutex::new(compaction_thread?),
            flush_thread: Mutex::new(None),
            observability,
        })
    }

    pub(super) fn series_shard_idx(series_id: SeriesId) -> usize {
        (series_id % IN_MEMORY_SHARD_COUNT as u64) as usize
    }

    pub(super) fn registry_metric_shard_idx(metric: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metric.hash(&mut hasher);
        (hasher.finish() as usize) % REGISTRY_TXN_SHARD_COUNT
    }

    pub(super) fn lock_registry_write_shards_for_rows<'a>(
        &'a self,
        rows: &[Row],
    ) -> Vec<MutexGuard<'a, ()>> {
        let mut shard_bits = [0u64; REGISTRY_TXN_SHARD_COUNT.div_ceil(u64::BITS as usize)];
        for row in rows {
            let shard_idx = Self::registry_metric_shard_idx(row.metric());
            let word_idx = shard_idx / (u64::BITS as usize);
            let bit_idx = shard_idx % (u64::BITS as usize);
            shard_bits[word_idx] |= 1u64 << bit_idx;
        }

        let guard_count = shard_bits
            .iter()
            .map(|bits| bits.count_ones() as usize)
            .sum();
        let mut guards = Vec::with_capacity(guard_count);
        for (word_idx, bits) in shard_bits.into_iter().enumerate() {
            let mut remaining = bits;
            while remaining != 0 {
                let bit_idx = remaining.trailing_zeros() as usize;
                let shard_idx = word_idx * (u64::BITS as usize) + bit_idx;
                guards.push(self.registry_write_txn_shards[shard_idx].lock());
                remaining &= remaining - 1;
            }
        }
        guards
    }

    pub(super) fn active_shard(
        &self,
        series_id: SeriesId,
    ) -> &RwLock<HashMap<SeriesId, ActiveSeriesState>> {
        &self.active_builders[Self::series_shard_idx(series_id)]
    }

    pub(super) fn sealed_shard(
        &self,
        series_id: SeriesId,
    ) -> &RwLock<HashMap<SeriesId, BTreeMap<SealedChunkKey, Chunk>>> {
        &self.sealed_chunks[Self::series_shard_idx(series_id)]
    }

    pub(super) fn mark_materialized_series_ids<I>(&self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.materialized_series.write().extend(series_ids);
    }

    pub(super) fn metric_series_for_ids<I>(&self, series_ids: I) -> Vec<MetricSeries>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let registry = self.registry.read();
        series_ids
            .into_iter()
            .filter_map(|series_id| {
                registry
                    .decode_series_key(series_id)
                    .map(|series_key| MetricSeries {
                        name: series_key.metric,
                        labels: series_key.labels,
                    })
            })
            .collect()
    }

    pub(super) fn ensure_open(&self) -> Result<()> {
        if self.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

    pub(super) fn update_max_observed_timestamp(&self, ts: i64) {
        let mut current = self.max_observed_timestamp.load(Ordering::Acquire);
        while ts > current {
            match self.max_observed_timestamp.compare_exchange_weak(
                current,
                ts,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    pub(super) fn append_sealed_chunk(&self, series_id: SeriesId, chunk: Chunk) {
        let shard_idx = Self::series_shard_idx(series_id);
        let account_memory = self.memory_accounting_enabled;
        let chunk_bytes = if account_memory {
            Self::chunk_memory_usage_bytes(&chunk)
        } else {
            0
        };
        let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
        let key = SealedChunkKey::from_chunk(&chunk, sequence);
        let mut sealed = self.sealed_shard(series_id).write();
        let replaced = sealed.entry(series_id).or_default().insert(key, chunk);
        if account_memory {
            self.add_memory_usage_bytes(shard_idx, chunk_bytes);
            if let Some(previous_chunk) = replaced.as_ref() {
                self.sub_memory_usage_bytes(
                    shard_idx,
                    Self::chunk_memory_usage_bytes(previous_chunk),
                );
            }
        }
    }

    pub(super) fn flush_all_active(&self) -> Result<()> {
        self.observability
            .flush
            .active_flush_runs_total
            .fetch_add(1, Ordering::Relaxed);

        let mut flushed_series = 0usize;
        let mut flushed_chunks = 0usize;
        let mut flushed_points = 0usize;

        let result = (|| -> Result<()> {
            let account_memory = self.memory_accounting_enabled;
            for (shard_idx, shard) in self.active_builders.iter().enumerate() {
                let mut active = shard.write();
                let mut active_added_bytes = 0usize;
                let mut active_removed_bytes = 0usize;
                let mut sealed_added_bytes = 0usize;
                let mut sealed_removed_bytes = 0usize;
                for (series_id, state) in active.iter_mut() {
                    let state_bytes_before = if account_memory {
                        Self::active_state_memory_usage_bytes(state)
                    } else {
                        0
                    };
                    let Some(chunk) = state.flush_partial()? else {
                        continue;
                    };
                    if account_memory {
                        let state_bytes_after = Self::active_state_memory_usage_bytes(state);
                        if state_bytes_after >= state_bytes_before {
                            active_added_bytes = active_added_bytes.saturating_add(
                                state_bytes_after.saturating_sub(state_bytes_before),
                            );
                        } else {
                            active_removed_bytes = active_removed_bytes.saturating_add(
                                state_bytes_before.saturating_sub(state_bytes_after),
                            );
                        }
                    }

                    flushed_series = flushed_series.saturating_add(1);
                    flushed_chunks = flushed_chunks.saturating_add(1);
                    flushed_points =
                        flushed_points.saturating_add(chunk.header.point_count as usize);
                    let chunk_bytes = if account_memory {
                        Self::chunk_memory_usage_bytes(&chunk)
                    } else {
                        0
                    };
                    let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
                    let key = SealedChunkKey::from_chunk(&chunk, sequence);
                    let mut sealed = self.sealed_shard(*series_id).write();
                    let replaced = sealed.entry(*series_id).or_default().insert(key, chunk);
                    if account_memory {
                        sealed_added_bytes = sealed_added_bytes.saturating_add(chunk_bytes);
                        if let Some(previous_chunk) = replaced.as_ref() {
                            sealed_removed_bytes = sealed_removed_bytes
                                .saturating_add(Self::chunk_memory_usage_bytes(previous_chunk));
                        }
                    }
                }
                if account_memory {
                    self.account_memory_delta_from_totals(
                        shard_idx,
                        active_added_bytes.saturating_add(sealed_added_bytes),
                        active_removed_bytes.saturating_add(sealed_removed_bytes),
                    );
                }
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                self.observability
                    .flush
                    .active_flushed_series_total
                    .fetch_add(saturating_u64_from_usize(flushed_series), Ordering::Relaxed);
                self.observability
                    .flush
                    .active_flushed_chunks_total
                    .fetch_add(saturating_u64_from_usize(flushed_chunks), Ordering::Relaxed);
                self.observability
                    .flush
                    .active_flushed_points_total
                    .fetch_add(saturating_u64_from_usize(flushed_points), Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .flush
                    .active_flush_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }
}
