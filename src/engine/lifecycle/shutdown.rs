use super::*;
use crate::engine::tombstone::TombstoneMap;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
struct LifecycleShutdownContext<'a> {
    background_maintenance_lock: &'a Mutex<()>,
    compaction_lock: &'a Mutex<()>,
    background: &'a BackgroundWorkerSupervisorState,
    write_limiter: &'a crate::concurrency::Semaphore,
    write_timeout: std::time::Duration,
    numeric_compactor: Option<&'a Compactor>,
    blob_compactor: Option<&'a Compactor>,
    tombstones: &'a RwLock<TombstoneMap>,
    pending_persisted_segment_diff: &'a Mutex<PendingPersistedSegmentDiff>,
    persisted_index_dirty: &'a AtomicBool,
    observability: &'a StorageObservabilityCounters,
    post_flush_replacement_data_path: Option<&'a Path>,
}

impl<'a> LifecycleShutdownContext<'a> {
    fn close_coordination_timeout(self, operation: &'static str) -> TsinkError {
        TsinkError::LifecycleTimeout {
            operation,
            timeout_ms: u64::try_from(self.write_timeout.as_millis()).unwrap_or(u64::MAX),
        }
    }

    fn acquire_close_gate(
        self,
        lock: &'a Mutex<()>,
        operation: &'static str,
    ) -> Result<MutexGuard<'a, ()>> {
        let started = Instant::now();
        let guard = if self.write_timeout.is_zero() {
            lock.try_lock()
        } else {
            lock.try_lock_for(self.write_timeout)
        };
        self.background
            .record_close_coordination_wait(started, guard.is_none());
        guard.ok_or_else(|| self.close_coordination_timeout(operation))
    }

    fn background_maintenance_gate(self) -> Result<MutexGuard<'a, ()>> {
        self.acquire_close_gate(
            self.background_maintenance_lock,
            "close background-maintenance drain",
        )
    }

    fn compaction_gate(self) -> Result<MutexGuard<'a, ()>> {
        self.acquire_close_gate(self.compaction_lock, "close compaction drain")
    }

    fn acquire_close_write_permits(self) -> Result<Vec<crate::concurrency::SemaphoreGuard<'a>>> {
        let started = Instant::now();
        let result = self.write_limiter.acquire_all(self.write_timeout);
        self.background
            .record_close_coordination_wait(started, result.is_err());
        result
    }

    fn compact_until_settled(self, max_passes: usize) -> Result<usize> {
        if self.numeric_compactor.is_none() && self.blob_compactor.is_none() {
            return Ok(0);
        }
        let _compaction_guard = self.compaction_gate()?;
        let mut passes = 0usize;
        for _ in 0..max_passes.max(1) {
            passes = passes.saturating_add(1);
            let changed = match ChunkStorage::compact_compactors_with_changes(
                self.post_flush_replacement_data_path,
                self.numeric_compactor,
                self.blob_compactor,
                Some(self.tombstones),
                Some(self.observability),
                |changes| {
                    self.pending_persisted_segment_diff.lock().merge(changes);
                    self.persisted_index_dirty.store(true, Ordering::SeqCst);
                },
            ) {
                Ok(changed) => changed,
                Err(err) => {
                    self.background.record_close_compaction_passes(passes);
                    return Err(err);
                }
            };
            if !changed {
                break;
            }
        }
        self.background.record_close_compaction_passes(passes);
        Ok(passes)
    }

    fn persisted_index_dirty(self) -> bool {
        self.persisted_index_dirty.load(Ordering::SeqCst)
    }

    fn record_deferred_dirty_refresh(self, err: &TsinkError) {
        self.observability
            .record_maintenance_error("close dirty persisted refresh", err);
    }
}

impl ChunkStorage {
    fn lifecycle_shutdown_context(&self) -> LifecycleShutdownContext<'_> {
        LifecycleShutdownContext {
            background_maintenance_lock: &self.coordination.background_maintenance_lock,
            compaction_lock: self.coordination.compaction_lock.as_ref(),
            background: &self.background,
            write_limiter: &self.runtime.write_limiter,
            write_timeout: self.runtime.write_timeout,
            numeric_compactor: self.persisted.numeric_compactor.as_ref(),
            blob_compactor: self.persisted.blob_compactor.as_ref(),
            tombstones: self.visibility.tombstones.as_ref(),
            pending_persisted_segment_diff: &self.persisted.pending_persisted_segment_diff,
            persisted_index_dirty: self.persisted.persisted_index_dirty.as_ref(),
            observability: self.observability.as_ref(),
            post_flush_replacement_data_path: self
                .persisted
                .series_index_path
                .as_deref()
                .and_then(Path::parent),
        }
    }

    pub(in super::super) fn compact_compactors_with_changes<F>(
        post_flush_replacement_data_path: Option<&Path>,
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        tombstones: Option<&RwLock<TombstoneMap>>,
        observability: Option<&StorageObservabilityCounters>,
        mut record_changes: F,
    ) -> Result<bool>
    where
        F: FnMut(PendingPersistedSegmentDiff),
    {
        if let Some(data_path) = post_flush_replacement_data_path {
            super::super::maintenance::ensure_no_pending_post_flush_replacement(data_path)?;
        }
        let mut changed = false;
        if let Some(compactor) = numeric_compactor {
            let changes = Self::run_compactor_once(compactor, tombstones, observability)?;
            if !changes.is_empty() {
                changed = true;
                record_changes(changes);
            }
        }
        if let Some(compactor) = blob_compactor {
            let changes = Self::run_compactor_once(compactor, tombstones, observability)?;
            if !changes.is_empty() {
                changed = true;
                record_changes(changes);
            }
        }
        Ok(changed)
    }

    pub(in super::super) fn compact_next_background_compactor_with_changes<F>(
        post_flush_replacement_data_path: Option<&Path>,
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        prefer_blob: bool,
        tombstones: Option<&RwLock<TombstoneMap>>,
        observability: Option<&StorageObservabilityCounters>,
        mut record_changes: F,
    ) -> Result<()>
    where
        F: FnMut(PendingPersistedSegmentDiff),
    {
        if let Some(data_path) = post_flush_replacement_data_path {
            super::super::maintenance::ensure_no_pending_post_flush_replacement(data_path)?;
        }

        // Numeric and blob compactors each carry the configured per-pass ceilings. Running both
        // in one worker wake silently doubled those ceilings, so alternate one available lane per
        // wake. A single-lane instance naturally selects its only compactor every time.
        let selected = if prefer_blob {
            blob_compactor.or(numeric_compactor)
        } else {
            numeric_compactor.or(blob_compactor)
        };
        let Some(compactor) = selected else {
            return Ok(());
        };
        let changes = Self::run_compactor_once(compactor, tombstones, observability)?;
        if !changes.is_empty() {
            record_changes(changes);
        }
        Ok(())
    }

    fn run_compactor_once(
        compactor: &Compactor,
        tombstones: Option<&RwLock<TombstoneMap>>,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<PendingPersistedSegmentDiff> {
        let started = Instant::now();
        let outcome = match tombstones {
            Some(tombstones) => {
                let tombstones = tombstones.read();
                compactor.compact_once_with_changes_using_tombstones(&tombstones)
            }
            None => compactor.compact_once_with_changes(),
        };
        match outcome {
            Ok(outcome) => {
                let stats = outcome.stats;
                if let Some(obs) = observability {
                    obs.record_compaction_result(stats, elapsed_nanos_u64(started));
                }
                let mut changes = PendingPersistedSegmentDiff::default();
                if stats.compacted {
                    changes.record_changes(outcome.output_roots, outcome.source_roots);
                }
                Ok(changes)
            }
            Err(err) => {
                if let Some(obs) = observability {
                    obs.record_compaction_error(elapsed_nanos_u64(started));
                }
                Err(err)
            }
        }
    }

    fn close_should_defer_dirty_refresh_error(err: &TsinkError) -> bool {
        crate::engine::segment::segment_validation_error_message(err).is_some()
            || matches!(err, TsinkError::Other(message) if message.contains("during runtime refresh"))
    }

    fn close_should_defer_resource_maintenance_error(err: &TsinkError) -> bool {
        matches!(
            err,
            TsinkError::DiskQuotaExceeded { .. }
                | TsinkError::InsufficientCompactionHeadroom { .. }
                | TsinkError::InsufficientDiskSpace { .. }
        )
    }

    fn execute_close_pipeline(&self) -> Result<()> {
        let shutdown = self.lifecycle_shutdown_context();
        let _background_maintenance_guard = shutdown.background_maintenance_gate()?;
        // A finite remote refresh may retain admitted tombstone fragments across wakes. Once the
        // background gate is exclusive, no worker can still use those continuations; release them
        // before any later close stage can time out so a failed/retried close does not pin memory.
        self.reset_bounded_catalog_refresh_continuations();
        self.reset_background_post_flush_recovery_cursor();
        self.reset_background_metadata_reconciliation_cursor();
        let mut deferred_dirty_refresh = false;
        let _write_permits = shutdown.acquire_close_write_permits()?;
        // All ingest and rollup materialization paths acquire their writer permit before this
        // transaction lock. Draining permits first gives close the same global lock order.
        let _rollup_transaction_guard = self.rollups.run_lock.lock();
        // Drain a compaction pass before any close stage enters a helper that takes the
        // compaction gate internally. Once the lifecycle is CLOSING, the worker rechecks that
        // state before running another pass, so later internal acquisitions are uncontended by
        // engine-owned compaction work. The guard is intentionally released because flush,
        // retention, and catalog helpers own their existing non-reentrant gate boundaries.
        drop(shutdown.compaction_gate()?);
        self.flush_all_active()?;
        self.persist_segment()?;
        if let Err(err) = self.sweep_expired_persisted_segments() {
            if Self::close_should_defer_resource_maintenance_error(&err) {
                shutdown.record_deferred_dirty_refresh(&err);
                tracing::warn!(
                    error = %err,
                    "Close deferred retention maintenance because disk headroom was unavailable"
                );
            } else {
                return Err(err);
            }
        }
        if let Err(err) = shutdown.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES) {
            if Self::close_should_defer_resource_maintenance_error(&err) {
                shutdown.record_deferred_dirty_refresh(&err);
                tracing::warn!(
                    error = %err,
                    "Close deferred compaction because disk headroom was unavailable"
                );
            } else {
                return Err(err);
            }
        }
        if shutdown.persisted_index_dirty() || self.has_known_persisted_segment_changes() {
            if let Err(err) = self.refresh_dirty_persisted_segments_claimed() {
                if Self::close_should_defer_dirty_refresh_error(&err) {
                    shutdown.record_deferred_dirty_refresh(&err);
                    tracing::warn!(
                        error = %err,
                        "Close deferred dirty persisted refresh and kept the last visible catalog"
                    );
                    deferred_dirty_refresh = true;
                } else {
                    return Err(err);
                }
            }
        }
        self.persist_tombstones_index_for_recovery_locked()?;
        if deferred_dirty_refresh {
            self.checkpoint_series_registry_index_allow_invalid_catalog_for_recovery()?;
        } else {
            self.checkpoint_series_registry_index_for_recovery()?;
        }
        Ok(())
    }

    pub(in super::super) fn close_impl(&self) -> Result<()> {
        let started = self.background.record_close_attempt();
        let close_result = match self.start_close_transition() {
            Ok(()) => self.finish_close_transition(self.execute_close_pipeline()),
            Err(err) => Err(err),
        };
        self.background
            .record_close_result(started, close_result.is_ok());
        close_result
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::config::ChunkStorageOptions;
    use super::*;
    use crate::engine::chunk::{ChunkHeader, ChunkPoint};
    use crate::engine::encoder::Encoder;
    use crate::engine::wal::FramedWal;
    use crate::{DataPoint, Row, Storage, Value, WalSyncMode};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn numeric_chunk(series_id: SeriesId, points: &[(i64, f64)]) -> Chunk {
        let points = points
            .iter()
            .map(|(ts, value)| ChunkPoint {
                ts: *ts,
                value: Value::F64(*value),
            })
            .collect::<Vec<_>>();
        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        Chunk {
            header: ChunkHeader {
                series_id,
                lane: ValueLane::Numeric,
                value_family: Some(SeriesValueFamily::F64),
                point_count: points.len() as u16,
                min_ts: points.first().unwrap().ts,
                max_ts: points.last().unwrap().ts,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points,
            encoded_payload: encoded.payload,
            wal_lowwater: WalHighWatermark::default(),
            wal_highwater: WalHighWatermark::default(),
        }
    }

    fn close_timeout_test_storage(temp: &TempDir) -> ChunkStorage {
        let wal =
            FramedWal::open(temp.path().join("wal"), WalSyncMode::PerAppend).expect("open WAL");
        ChunkStorage::new_with_data_path_and_options(
            64,
            Some(wal),
            Some(temp.path().join("lane_numeric")),
            None,
            1,
            ChunkStorageOptions {
                write_timeout: Duration::ZERO,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .expect("build close-timeout test storage")
    }

    #[test]
    fn close_pipeline_keeps_memory_accounting_incremental() {
        let temp = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp.path().join("wal"), WalSyncMode::PerAppend).expect("open WAL");
        let storage = ChunkStorage::new_with_data_path_and_options(
            64,
            Some(wal),
            Some(temp.path().join("lane_numeric")),
            None,
            1,
            ChunkStorageOptions {
                memory_budget_bytes: 32 * 1024 * 1024,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .expect("build close accounting test storage");
        Storage::insert_rows(
            &storage,
            &[Row::new(
                "close_incremental_accounting",
                DataPoint::new(1, 7.0),
            )],
        )
        .expect("the WAL-backed write should be accepted");
        assert!(
            storage
                .memory_observability_snapshot()
                .wal_series_definition_cache_bytes
                > 0
        );

        let full_reconciliations = Arc::new(AtomicUsize::new(0));
        storage.set_full_memory_reconciliation_hook({
            let full_reconciliations = Arc::clone(&full_reconciliations);
            move || {
                full_reconciliations.fetch_add(1, Ordering::Relaxed);
            }
        });
        storage.close_impl().expect("close should finish");
        assert_eq!(
            full_reconciliations.load(Ordering::Relaxed),
            0,
            "close must rely on the flush, retention, and catalog incremental deltas"
        );
        let closed_memory = storage.memory_observability_snapshot();
        let exact_cache_bytes = storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .cached_series_definition_index_memory_usage_bytes();
        assert_eq!(
            closed_memory.wal_series_definition_cache_bytes,
            exact_cache_bytes
        );

        storage.clear_full_memory_reconciliation_hook();
        super::super::super::tests::assert_engine_memory_usage_reconciled(&storage);
    }

    #[test]
    fn close_times_out_at_background_gate_and_retry_preserves_accepted_data() {
        let temp = TempDir::new().unwrap();
        let storage = close_timeout_test_storage(&temp);
        Storage::insert_rows(
            &storage,
            &[Row::new("close_gate_timeout", DataPoint::new(1, 7.0))],
        )
        .expect("the WAL-backed write should be accepted");

        let held_gate = storage.coordination.background_maintenance_lock.lock();
        let error = storage
            .close_impl()
            .expect_err("close must not wait indefinitely for the maintenance gate");
        assert!(matches!(
            error,
            TsinkError::LifecycleTimeout {
                operation: "close background-maintenance drain",
                timeout_ms: 0
            }
        ));
        assert_eq!(
            storage.coordination.lifecycle.load(Ordering::SeqCst),
            super::super::super::STORAGE_OPEN,
            "a pre-publication timeout must reopen the instance for a safe retry"
        );
        let timed_out = storage.observability_snapshot_impl().background;
        assert_eq!(timed_out.close_attempts_total, 1);
        assert_eq!(timed_out.close_success_total, 0);
        assert_eq!(timed_out.close_errors_total, 1);
        assert_eq!(timed_out.close_coordination_timeouts_total, 1);

        drop(held_gate);
        storage.close_impl().expect("close retry should finish");

        let closed = storage.observability_snapshot_impl().background;
        assert_eq!(closed.close_attempts_total, 2);
        assert_eq!(closed.close_success_total, 1);
        assert_eq!(closed.close_errors_total, 1);
        assert_eq!(closed.close_coordination_timeouts_total, 1);
        assert_eq!(closed.close_compaction_passes_total, 1);
        assert_eq!(
            closed.close_compaction_pass_limit,
            CLOSE_COMPACTION_MAX_PASSES as u64
        );

        let persisted =
            crate::engine::segment::load_segments(temp.path().join("lane_numeric")).unwrap();
        let points = persisted
            .chunks_by_series
            .values()
            .flatten()
            .flat_map(|chunk| chunk.decode_points().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 1);
        assert_eq!(points[0].value, Value::F64(7.0));
    }

    #[test]
    fn close_times_out_at_compaction_gate_without_entering_compaction() {
        let temp = TempDir::new().unwrap();
        let storage = close_timeout_test_storage(&temp);
        Storage::insert_rows(
            &storage,
            &[Row::new("close_compaction_timeout", DataPoint::new(2, 9.0))],
        )
        .expect("the WAL-backed write should be accepted");

        let held_gate = storage.coordination.compaction_lock.lock();
        let error = storage
            .close_impl()
            .expect_err("close must not wait indefinitely for the compaction gate");
        assert!(matches!(
            error,
            TsinkError::LifecycleTimeout {
                operation: "close compaction drain",
                timeout_ms: 0
            }
        ));
        assert_eq!(
            storage.coordination.lifecycle.load(Ordering::SeqCst),
            super::super::super::STORAGE_OPEN
        );
        let timed_out = storage.observability_snapshot_impl().background;
        assert_eq!(timed_out.close_coordination_timeouts_total, 1);
        assert_eq!(timed_out.close_compaction_passes_total, 0);
        assert_eq!(
            Storage::select(&storage, "close_compaction_timeout", &[], 0, 3).unwrap(),
            vec![DataPoint::new(2, 9.0)],
            "the pre-persistence timeout must leave accepted state queryable for retry"
        );
        assert!(
            crate::engine::segment::load_segments(temp.path().join("lane_numeric"))
                .unwrap()
                .chunks_by_series
                .is_empty(),
            "the timed compaction preflight must run before segment publication"
        );

        drop(held_gate);
        storage.close_impl().expect("close retry should finish");
    }

    #[test]
    fn later_lane_failure_does_not_drop_an_earlier_committed_diff() {
        let temp = TempDir::new().unwrap();
        let numeric_path = temp.path().join("numeric");
        let blob_path = temp.path().join("blob");
        let registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap()
            .series_id;

        for (segment_id, points) in [
            (1, vec![(10, 1.0), (20, 2.0)]),
            (2, vec![(15, 3.0), (30, 4.0)]),
        ] {
            let chunks = HashMap::from([(series_id, vec![numeric_chunk(series_id, &points)])]);
            SegmentWriter::new(&numeric_path, 0, segment_id)
                .unwrap()
                .write_segment(&registry, &chunks)
                .unwrap();
        }

        let corrupt_blob_root = blob_path
            .join("segments")
            .join("L0")
            .join("seg-0000000000000001");
        std::fs::create_dir_all(&corrupt_blob_root).unwrap();
        std::fs::write(corrupt_blob_root.join("manifest.bin"), b"not-a-manifest").unwrap();

        let numeric = Compactor::new(&numeric_path, 8);
        let blob = Compactor::new(&blob_path, 8);
        let mut recorded = PendingPersistedSegmentDiff::default();
        let error = ChunkStorage::compact_compactors_with_changes(
            None,
            Some(&numeric),
            Some(&blob),
            None,
            None,
            |changes| recorded.merge(changes),
        )
        .expect_err("the corrupt later lane must fail");

        assert!(crate::engine::segment::segment_validation_error_message(&error).is_some());
        assert_eq!(recorded.removed_roots.len(), 2);
        assert_eq!(recorded.added_roots.len(), 1);
        assert!(recorded.removed_roots.iter().all(|root| !root.exists()));
        assert!(recorded.added_roots.iter().all(|root| root.exists()));
    }

    #[test]
    fn background_compaction_runs_only_the_selected_lane_per_pass() {
        let temp = TempDir::new().unwrap();
        let numeric_path = temp.path().join("numeric");
        let blob_path = temp.path().join("blob");
        std::fs::create_dir_all(&numeric_path).unwrap();

        let corrupt_blob_root = blob_path
            .join("segments")
            .join("L0")
            .join("seg-0000000000000001");
        std::fs::create_dir_all(&corrupt_blob_root).unwrap();
        std::fs::write(corrupt_blob_root.join("manifest.bin"), b"not-a-manifest").unwrap();

        let numeric = Compactor::new(&numeric_path, 8);
        let blob = Compactor::new(&blob_path, 8);
        ChunkStorage::compact_next_background_compactor_with_changes(
            None,
            Some(&numeric),
            Some(&blob),
            false,
            None,
            None,
            |_| {},
        )
        .expect("the numeric turn must not inspect the corrupt blob lane");

        let error = ChunkStorage::compact_next_background_compactor_with_changes(
            None,
            Some(&numeric),
            Some(&blob),
            true,
            None,
            None,
            |_| {},
        )
        .expect_err("the following blob turn must inspect its selected corrupt lane");
        assert!(crate::engine::segment::segment_validation_error_message(&error).is_some());
    }

    #[test]
    fn engine_compaction_uses_authoritative_live_tombstones() {
        let temp = TempDir::new().unwrap();
        let numeric_path = temp.path().join("numeric");
        let registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "live-tombstone")])
            .unwrap()
            .series_id;
        for (segment_id, points) in [
            (1, vec![(10, 1.0), (20, 2.0)]),
            (2, vec![(15, 3.0), (30, 4.0)]),
        ] {
            let chunks = HashMap::from([(series_id, vec![numeric_chunk(series_id, &points)])]);
            SegmentWriter::new(&numeric_path, 0, segment_id)
                .unwrap()
                .write_segment(&registry, &chunks)
                .unwrap();
        }
        assert!(
            !numeric_path
                .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME)
                .exists(),
            "the regression must not fall back to a durable tombstone file"
        );
        let tombstones = RwLock::new(TombstoneMap::from([(
            series_id,
            vec![crate::engine::tombstone::TombstoneRange { start: 15, end: 21 }],
        )]));
        let compactor = Compactor::new(&numeric_path, 8);

        assert!(ChunkStorage::compact_compactors_with_changes(
            None,
            Some(&compactor),
            None,
            Some(&tombstones),
            None,
            |_| {},
        )
        .unwrap());

        let loaded = crate::engine::segment::load_segments(&numeric_path).unwrap();
        let mut timestamps = loaded.chunks_by_series[&series_id]
            .iter()
            .flat_map(|chunk| chunk.decode_points().unwrap())
            .map(|point| point.ts)
            .collect::<Vec<_>>();
        timestamps.sort_unstable();
        assert_eq!(timestamps, vec![10, 30]);
    }

    #[test]
    fn regular_compaction_is_fenced_until_prepared_recovery_completes() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let numeric_path = data_path.join("lane_numeric");
        let registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "fenced")])
            .unwrap()
            .series_id;
        let mut source_roots = Vec::new();
        for (segment_id, points) in [
            (1, vec![(10, 1.0), (20, 2.0)]),
            (2, vec![(15, 3.0), (30, 4.0)]),
            (3, vec![(40, 5.0), (50, 6.0)]),
        ] {
            let chunks = HashMap::from([(series_id, vec![numeric_chunk(series_id, &points)])]);
            let writer = SegmentWriter::new(&numeric_path, 0, segment_id).unwrap();
            writer.write_segment(&registry, &chunks).unwrap();
            source_roots.push(writer.layout().root.clone());
        }

        let marker_dir =
            data_path.join(super::super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME);
        std::fs::create_dir_all(&marker_dir).unwrap();
        let write_marker = |marker_id: &str, phase: &str, source_id: u64| {
            let marker_path = marker_dir.join(format!("transaction-{marker_id}.json"));
            std::fs::write(
                &marker_path,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "phase": phase,
                    "id": marker_id,
                    "sources": [{
                        "segment": {
                            "lane": "numeric",
                            "tier": "hot",
                            "relative_path": format!("segments/L0/seg-{source_id:016x}")
                        },
                        "counts_as_expired": false
                    }],
                    "outputs": [],
                    "tier_moves": 0
                }))
                .unwrap(),
            )
            .unwrap();
            marker_path
        };
        let prepared_marker = write_marker("0000000000000001-0000000000000002", "prepared", 1);

        let compactor = Compactor::new(&numeric_path, 8);
        let error = ChunkStorage::compact_compactors_with_changes(
            Some(&data_path),
            Some(&compactor),
            None,
            None,
            None,
            |_| panic!("a fenced compactor must not publish a diff"),
        )
        .expect_err("the pending Prepared marker must fence regular compaction");
        assert!(error
            .to_string()
            .contains("post-flush replacement is pending"));
        assert!(source_roots.iter().all(|root| root.is_dir()));

        super::super::super::maintenance::finalize_pending_post_flush_replacements_for_startup(
            &data_path,
            Some(&numeric_path),
            None,
            None,
            None,
            usize::MAX,
        )
        .unwrap();
        assert!(!prepared_marker.exists());
        assert!(source_roots.iter().all(|root| root.is_dir()));

        let committing_marker = write_marker("0000000000000003-0000000000000004", "committing", 3);
        let error = ChunkStorage::compact_compactors_with_changes(
            Some(&data_path),
            Some(&compactor),
            None,
            None,
            None,
            |_| panic!("a fenced compactor must not publish a diff"),
        )
        .expect_err("the pending Committing marker must fence regular compaction");
        assert!(error
            .to_string()
            .contains("post-flush replacement is pending"));
        assert!(source_roots.iter().all(|root| root.is_dir()));

        super::super::super::maintenance::finalize_pending_post_flush_replacements_for_startup(
            &data_path,
            Some(&numeric_path),
            None,
            None,
            None,
            usize::MAX,
        )
        .unwrap();
        assert!(!committing_marker.exists());
        assert!(source_roots[0].is_dir());
        assert!(source_roots[1].is_dir());
        assert!(!source_roots[2].exists());

        let mut recorded = PendingPersistedSegmentDiff::default();
        assert!(ChunkStorage::compact_compactors_with_changes(
            Some(&data_path),
            Some(&compactor),
            None,
            None,
            None,
            |changes| recorded.merge(changes),
        )
        .unwrap());
        assert_eq!(recorded.removed_roots.len(), 2);
        assert_eq!(recorded.added_roots.len(), 1);
    }
}
