use super::codec::{
    decode_samples_payload, decode_series_definition, validate_frame_payload_structure,
};
use super::replay::{parse_frame_header, read_header, HeaderRead, ParsedFrameHeader};
use super::*;

#[derive(Debug, Clone)]
pub(super) struct WalSegmentFile {
    pub(super) id: u64,
    pub(super) path: PathBuf,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct WalRuntimeAccounting {
    pub(super) total_size_bytes: u64,
    pub(super) segment_count: u64,
    pub(super) active_segment_size_bytes: u64,
}

impl WalRuntimeAccounting {
    fn from_segments(segments: &[WalSegmentFile]) -> Result<Self> {
        let mut total_size_bytes = 0u64;
        let mut active_segment_size_bytes = 0u64;
        for (idx, segment) in segments.iter().enumerate() {
            match fs::metadata(&segment.path) {
                Ok(meta) => {
                    let len = meta.len();
                    total_size_bytes = total_size_bytes.saturating_add(len);
                    if idx + 1 == segments.len() {
                        active_segment_size_bytes = len;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(Self {
            total_size_bytes,
            segment_count: segments.len() as u64,
            active_segment_size_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RecoverableSegmentScan {
    max_seq: u64,
    encountered_corruption: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WalOpenRecoveryState {
    last_highwater: WalHighWatermark,
    active_segment_last_seq: u64,
    quarantine_active_segment: bool,
}

const LEGACY_WAL_IDENTITY_FIXED_MEMORY_BYTES: usize = 16 * 1024;

struct WalOpenConfiguration {
    sync_mode: WalSyncMode,
    buffer_size: usize,
    segment_max_bytes: u64,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    replay_mode: WalReplayMode,
    allow_namespace_creation: bool,
    replay_highwater: Option<WalHighWatermark>,
}

impl FramedWal {
    pub(in crate::engine) fn write_buffer_capacity_bytes(&self) -> usize {
        self.writer.lock().capacity()
    }

    pub fn open(dir: impl AsRef<Path>, sync_mode: WalSyncMode) -> Result<Self> {
        Self::open_with_buffer_size(dir, sync_mode, DEFAULT_WAL_BUFFER_SIZE)
    }

    pub fn open_with_buffer_size(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
    ) -> Result<Self> {
        Self::open_with_options(dir, sync_mode, buffer_size, DEFAULT_WAL_SEGMENT_MAX_BYTES)
    }

    #[cfg(test)]
    pub(in crate::engine) fn open_with_buffer_size_and_disk_budget(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Result<Self> {
        Self::open_with_buffer_size_and_disk_budget_and_replay_mode(
            dir,
            sync_mode,
            buffer_size,
            local_disk_budget,
            WalReplayMode::Strict,
        )
    }

    #[cfg(test)]
    pub(in crate::engine) fn open_with_buffer_size_and_disk_budget_and_replay_mode(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
        replay_mode: WalReplayMode,
    ) -> Result<Self> {
        Self::open_with_buffer_size_and_disk_budget_and_replay_mode_and_creation(
            dir,
            sync_mode,
            buffer_size,
            local_disk_budget,
            replay_mode,
            true,
            None,
        )
    }

    #[cfg(test)]
    pub(in crate::engine) fn open_with_buffer_size_and_disk_budget_and_replay_floor(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
        replay_mode: WalReplayMode,
        replay_highwater: WalHighWatermark,
    ) -> Result<Self> {
        Self::open_with_buffer_size_and_disk_budget_and_replay_mode_and_creation(
            dir,
            sync_mode,
            buffer_size,
            local_disk_budget,
            replay_mode,
            true,
            Some(replay_highwater),
        )
    }

    pub(in crate::engine) fn open_with_buffer_size_and_disk_budget_and_replay_mode_and_creation(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
        replay_mode: WalReplayMode,
        allow_namespace_creation: bool,
        replay_highwater: Option<WalHighWatermark>,
    ) -> Result<Self> {
        Self::open_with_options_and_disk_budget(
            dir,
            WalOpenConfiguration {
                sync_mode,
                buffer_size,
                segment_max_bytes: DEFAULT_WAL_SEGMENT_MAX_BYTES,
                local_disk_budget,
                replay_mode,
                allow_namespace_creation,
                replay_highwater,
            },
        )
    }

    pub(crate) fn open_with_options(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_options_and_disk_budget(
            dir,
            WalOpenConfiguration {
                sync_mode,
                buffer_size,
                segment_max_bytes,
                local_disk_budget: None,
                replay_mode: WalReplayMode::Strict,
                allow_namespace_creation: true,
                replay_highwater: None,
            },
        )
    }

    fn open_with_options_and_disk_budget(
        dir: impl AsRef<Path>,
        configuration: WalOpenConfiguration,
    ) -> Result<Self> {
        let WalOpenConfiguration {
            sync_mode,
            buffer_size,
            segment_max_bytes,
            local_disk_budget,
            replay_mode,
            allow_namespace_creation,
            replay_highwater,
        } = configuration;
        let dir = dir.as_ref().to_path_buf();
        if allow_namespace_creation {
            fs::create_dir_all(&dir)?;
        } else {
            let metadata = fs::symlink_metadata(&dir).map_err(|source| TsinkError::IoWithPath {
                path: dir.clone(),
                source,
            })?;
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_dir()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "non-creating WAL open requires an existing plain directory: {}",
                    dir.display()
                )));
            }
        }

        let mut segments = collect_wal_segment_files(&dir)?;
        if segments.is_empty() {
            if !allow_namespace_creation {
                return Err(TsinkError::DataCorruption(format!(
                    "non-creating WAL open requires an existing canonical segment: {}",
                    dir.display()
                )));
            }
            let path = segment_path(&dir, 0);
            let (file, created, _) = open_segment_for_append(&path)?;
            if !created {
                return Err(TsinkError::DataCorruption(format!(
                    "new WAL namespace raced with an existing segment path: {}",
                    path.display(),
                )));
            }
            drop(file);
            sync_dir_path(&dir)?;
            segments.push(WalSegmentFile { id: 0, path });
        }

        let mut active = segments.last().cloned().ok_or_else(|| TsinkError::Wal {
            operation: "open".to_string(),
            details: "missing WAL segment after initialization".to_string(),
        })?;
        let published_highwater_path = published_highwater_path(&dir);
        let published_highwater_tmp_path = published_highwater_tmp_path(&dir);
        let existing_published_record = read_published_highwater(&published_highwater_path)?;
        if existing_published_record.is_none() && !allow_namespace_creation {
            return Err(TsinkError::DataCorruption(format!(
                "non-creating WAL open requires the canonical publish-boundary marker: {}",
                published_highwater_path.display()
            )));
        }
        if let Some(published_record) = existing_published_record {
            let effective_replay_highwater = replay_highwater
                .unwrap_or_default()
                .max(published_record.reset_through.unwrap_or_default());
            validate_required_published_segment_coverage(
                &segments,
                effective_replay_highwater,
                published_record.highwater,
            )?;
            discard_unpublished_suffixes(
                &segments,
                published_record,
                replay_highwater.unwrap_or_default(),
            )?;
        } else {
            // A legacy namespace has no separate publication boundary, so every existing byte is
            // part of the only durable prefix it can describe. Validate that complete prefix
            // before deriving and publishing a current-format marker. In particular, Salvage must
            // not quarantine a corrupt legacy segment or make the damaged source writable in
            // place.
            validate_markerless_published_segments(&segments)?;
        }
        let recovery = scan_segments_for_open(&segments)?;
        let mut active_last_seq = recovery.active_segment_last_seq;
        let mut last_highwater = recovery.last_highwater;
        if existing_published_record.is_none() {
            if let Some(replay_highwater) = replay_highwater {
                validate_required_published_segment_coverage(
                    &segments,
                    replay_highwater,
                    last_highwater,
                )?;
            }
        }
        if let Some(published_record) = existing_published_record {
            last_highwater = last_highwater.max(published_record.highwater);
            if active.id == published_record.highwater.segment {
                active_last_seq = active_last_seq.max(published_record.highwater.frame);
            }
        }
        if recovery.quarantine_active_segment && replay_mode == WalReplayMode::Strict {
            return Err(TsinkError::DataCorruption(format!(
                "strict WAL open detected corruption in active segment {} at {}",
                active.id,
                active.path.display()
            )));
        }

        let published_highwater = existing_published_record
            .map(|record| record.highwater)
            .unwrap_or(last_highwater);
        let writer_file =
            if recovery.quarantine_active_segment && replay_mode == WalReplayMode::Salvage {
                let quarantined_segment = active.id;
                let next_segment = quarantined_segment.saturating_add(1);
                let next_path = segment_path(&dir, next_segment);
                let (file, segment_created, _) = open_segment_for_append(&next_path)?;
                if segment_created {
                    sync_dir_path(&dir)?;
                }
                warn!(
                    segment = quarantined_segment,
                    path = %active.path.display(),
                    next_segment,
                    "WAL open quarantined corrupted active segment"
                );
                active = WalSegmentFile {
                    id: next_segment,
                    path: next_path,
                };
                segments.push(active.clone());
                active_last_seq = 0;
                file
            } else {
                let (file, _, _) = open_segment_for_append(&active.path)?;
                file
            };
        let accounting = WalRuntimeAccounting::from_segments(&segments)?;
        let writer = BufWriter::with_capacity(buffer_size.max(1), writer_file);

        let wal = Self {
            dir,
            path: Mutex::new(active.path.clone()),
            published_highwater_path,
            published_highwater_tmp_path,
            writer: Mutex::new(writer),
            active_segment: AtomicU64::new(active.id),
            active_segment_size_bytes: AtomicU64::new(accounting.active_segment_size_bytes),
            next_seq: AtomicU64::new(active_last_seq.saturating_add(1)),
            total_size_bytes: AtomicU64::new(accounting.total_size_bytes),
            segment_count: AtomicU64::new(accounting.segment_count),
            cached_series_definition_index: Mutex::new(CachedSeriesDefinitionIndex::default()),
            cached_series_definition_index_ready: Condvar::new(),
            last_appended_highwater: Mutex::new(last_highwater),
            last_published_highwater: Mutex::new(published_highwater),
            reset_highwater_floor: Mutex::new(
                existing_published_record.and_then(|record| record.reset_through),
            ),
            last_durable_highwater: Mutex::new(last_highwater),
            configured_replay_mode: Mutex::new(replay_mode),
            sync_mode,
            last_sync: Mutex::new(Instant::now()),
            segment_max_bytes: segment_max_bytes.max(1),
            local_disk_budget,
            #[cfg(test)]
            append_sync_hook: Mutex::new(None),
            #[cfg(test)]
            published_highwater_post_rename_hook: Mutex::new(None),
            #[cfg(test)]
            cached_series_definition_rebuild_hook: Mutex::new(None),
            #[cfg(test)]
            durability_failpoint_hook: Mutex::new(None),
        };

        if existing_published_record.is_none() {
            wal.persist_published_highwater_with_recovery_budget(published_highwater, true)?;
        }

        Ok(wal)
    }

    pub fn path(&self) -> PathBuf {
        self.path.lock().clone()
    }

    fn store_runtime_accounting(&self, accounting: WalRuntimeAccounting) {
        self.active_segment_size_bytes
            .store(accounting.active_segment_size_bytes, Ordering::Release);
        self.total_size_bytes
            .store(accounting.total_size_bytes, Ordering::Release);
        self.segment_count
            .store(accounting.segment_count, Ordering::Release);
    }

    pub(super) fn refresh_runtime_accounting(&self) -> Result<()> {
        self.store_runtime_accounting(scan_wal_runtime_accounting(&self.dir)?);
        Ok(())
    }

    pub(super) fn record_segment_created(&self, initial_size_bytes: u64) {
        self.segment_count.fetch_add(1, Ordering::AcqRel);
        if initial_size_bytes > 0 {
            self.total_size_bytes
                .fetch_add(initial_size_bytes, Ordering::AcqRel);
        }
    }

    pub(super) fn record_appended_bytes(&self, appended_bytes: u64) {
        if appended_bytes > 0 {
            self.total_size_bytes
                .fetch_add(appended_bytes, Ordering::AcqRel);
        }
    }

    pub(super) fn active_segment_size_bytes(&self) -> u64 {
        self.active_segment_size_bytes.load(Ordering::Acquire)
    }

    pub fn total_size_bytes(&self) -> Result<u64> {
        Ok(self.total_size_bytes.load(Ordering::Acquire))
    }

    pub fn active_segment(&self) -> u64 {
        self.active_segment.load(Ordering::Acquire)
    }

    pub fn segment_count(&self) -> Result<u64> {
        Ok(self.segment_count.load(Ordering::Acquire))
    }

    fn reset_locked(
        &self,
        mut writer: MutexGuard<'_, BufWriter<File>>,
        observe_reset_cache: impl FnOnce(usize),
    ) -> Result<()> {
        let mut disk_reservation = self
            .local_disk_budget
            .as_ref()
            .map(|budget| {
                budget.reserve(
                    crate::DiskCategory::Wal,
                    PUBLISHED_HIGHWATER_MAX_RECORD_LEN as u64,
                    crate::DiskReservationKind::Recovery,
                )
            })
            .transpose()?;
        // Rotation may already have installed a newer empty active segment. Advancing the reset
        // floor to that segment's zero-frame boundary makes the retained reset anchor explicit and
        // ensures subsequent frame numbering is strictly above it.
        let reset_highwater = self.current_appended_highwater().max(WalHighWatermark {
            segment: self.active_segment.load(Ordering::SeqCst),
            frame: 0,
        });
        let reset_result = (|| -> Result<()> {
            writer.flush()?;
            writer.get_mut().sync_data()?;
            // Authorize the reset durably before removing any published frame. A crash after this
            // point can safely finish the reset during recovery; a crash before it leaves the
            // ordinary commit marker and its exact published prefix intact.
            self.persist_reset_highwater(reset_highwater, true)?;
            #[cfg(test)]
            self.invoke_durability_failpoint(WalDurabilityFailpoint::ResetAfterMarkerSync)?;
            let active_path = self.path.lock().clone();
            let replacement = open_existing_wal_segment_with(&active_path, |options| {
                options.write(true).truncate(true);
            })?;
            let capacity = writer.capacity();
            let old_writer = std::mem::replace(
                &mut *writer,
                BufWriter::with_capacity(capacity, replacement),
            );
            let _ = old_writer.into_parts();
            writer.get_ref().sync_data()?;
            #[cfg(test)]
            self.invoke_durability_failpoint(WalDurabilityFailpoint::ResetAfterTruncate)?;

            for segment in collect_wal_segment_files(&self.dir)? {
                if segment.path == active_path {
                    continue;
                }

                match fs::remove_file(&segment.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }

            sync_dir_path(&self.dir)?;
            Ok(())
        })();

        if reset_result.is_ok() {
            // Publish the exact post-clear cache charge while the cache mutex is still held.
            // Growth observations use the same mutex, so an older observation cannot re-add a
            // stale charge after this reset releases it. Do this before settlement/reconciliation:
            // those later stages can fail after the cache was already cleared.
            self.clear_cached_series_definition_index_if_initialized(observe_reset_cache);
            self.advance_highwater_floor(reset_highwater);
            *self.last_sync.lock() = Instant::now();
        }

        // Conservatively charge the complete marker until the exclusive scan below replaces all
        // WAL accounting with the exact post-reset tree. Settling first is required because an
        // active reservation would prevent idle reconciliation from beginning.
        let settlement_result = match disk_reservation.take() {
            Some(reservation) => reservation.commit(PUBLISHED_HIGHWATER_MAX_RECORD_LEN as u64, 0),
            None => Ok(()),
        };
        let runtime_reconciliation_result = self.refresh_runtime_accounting();
        let disk_reconciliation_result = match &self.local_disk_budget {
            Some(budget) => budget.reconcile_when_idle().map(|_| ()),
            None => Ok(()),
        };
        drop(writer);

        let mut errors = Vec::new();
        if let Err(err) = &reset_result {
            errors.push(format!("reset operation failed: {err}"));
        }
        if let Err(err) = &settlement_result {
            errors.push(format!("disk settlement failed: {err}"));
        }
        if let Err(err) = &runtime_reconciliation_result {
            errors.push(format!("runtime accounting reconciliation failed: {err}"));
        }
        if let Err(err) = &disk_reconciliation_result {
            errors.push(format!("local disk reconciliation failed: {err}"));
        }

        if errors.is_empty() {
            return Ok(());
        }
        if errors.len() == 1 {
            return match (
                reset_result,
                settlement_result,
                runtime_reconciliation_result,
                disk_reconciliation_result,
            ) {
                (Err(err), _, _, _)
                | (_, Err(err), _, _)
                | (_, _, Err(err), _)
                | (_, _, _, Err(err)) => Err(err),
                _ => unreachable!("one recorded WAL reset error must match one failed result"),
            };
        }

        Err(TsinkError::Other(format!(
            "WAL reset failed: {}",
            errors.join("; ")
        )))
    }

    pub fn reset(&self) -> Result<()> {
        self.reset_locked(self.writer.lock(), |_| {})
    }

    pub(crate) fn reset_if_current_highwater_at_most(
        &self,
        max_highwater: WalHighWatermark,
        observe_reset_cache: impl FnOnce(usize),
    ) -> Result<bool> {
        let writer = self.writer.lock();
        if *self.last_appended_highwater.lock() > max_highwater {
            return Ok(false);
        }

        self.reset_locked(writer, observe_reset_cache)?;
        Ok(true)
    }

    pub fn ensure_min_next_seq(&self, min_next_seq: u64) {
        let mut current = self.next_seq.load(Ordering::SeqCst);
        while current < min_next_seq {
            match self.next_seq.compare_exchange_weak(
                current,
                min_next_seq,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        if min_next_seq == 0 {
            return;
        }

        let floor = WalHighWatermark {
            segment: self.active_segment.load(Ordering::SeqCst),
            frame: min_next_seq.saturating_sub(1),
        };
        self.advance_highwater_floor(floor);
    }

    pub fn ensure_min_highwater(&self, min_highwater: WalHighWatermark) -> Result<()> {
        self.ensure_min_highwater_inner(min_highwater, true)
    }

    pub(in crate::engine) fn ensure_min_highwater_without_segment_creation(
        &self,
        min_highwater: WalHighWatermark,
    ) -> Result<()> {
        self.ensure_min_highwater_inner(min_highwater, false)
    }

    fn ensure_min_highwater_inner(
        &self,
        min_highwater: WalHighWatermark,
        allow_segment_creation: bool,
    ) -> Result<()> {
        let mut writer = self.writer.lock();
        let mut active_segment = self.active_segment.load(Ordering::SeqCst);
        let mut next_seq = self.next_seq.load(Ordering::SeqCst);

        if active_segment < min_highwater.segment {
            if !allow_segment_creation {
                return Err(TsinkError::DataCorruption(format!(
                    "WAL active segment {active_segment} is below required persisted high-watermark segment {} and validation cannot create recovery paths",
                    min_highwater.segment
                )));
            }
            let path = segment_path(&self.dir, min_highwater.segment);
            let (replacement, segment_created, initial_len) = open_segment_for_append(&path)?;
            let capacity = writer.capacity();
            let old_writer = std::mem::replace(
                &mut *writer,
                BufWriter::with_capacity(capacity, replacement),
            );
            let _ = old_writer.into_parts();

            active_segment = min_highwater.segment;
            next_seq = 1;
            self.active_segment.store(active_segment, Ordering::SeqCst);
            self.active_segment_size_bytes
                .store(initial_len, Ordering::Release);
            *self.path.lock() = path;
            if segment_created {
                self.record_segment_created(initial_len);
            }
            sync_dir_path(&self.dir)?;
        }

        if active_segment == min_highwater.segment && next_seq <= min_highwater.frame {
            next_seq = min_highwater.frame.saturating_add(1);
        }

        self.next_seq.store(next_seq, Ordering::SeqCst);
        self.advance_highwater_floor(min_highwater);

        Ok(())
    }
}

fn published_highwater_path(dir: &Path) -> PathBuf {
    dir.join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)
}

fn published_highwater_tmp_path(dir: &Path) -> PathBuf {
    dir.join(WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME)
}

fn require_plain_published_highwater_metadata(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if crate::engine::fs_utils::is_link_or_reparse_point(metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker must be a regular non-link file: {}",
            path.display(),
        )));
    }
    if metadata.len() != PUBLISHED_HIGHWATER_RECORD_LEN as u64
        && metadata.len() != PUBLISHED_HIGHWATER_V2_RECORD_LEN as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary record must be {PUBLISHED_HIGHWATER_RECORD_LEN} or {PUBLISHED_HIGHWATER_V2_RECORD_LEN} bytes, found {}: {}",
            metadata.len(),
            path.display(),
        )));
    }
    Ok(())
}

fn read_published_highwater(path: &Path) -> Result<Option<PublishedHighwaterRecord>> {
    let initial_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    require_plain_published_highwater_metadata(path, &initial_metadata)?;
    let initial_identity =
        same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;

    let mut options = OpenOptions::new();
    options.read(true);
    configure_wal_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let opened_metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    require_plain_published_highwater_metadata(path, &opened_metadata)?;
    if opened_metadata.len() != initial_metadata.len() {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker changed length while opening: {}",
            path.display(),
        )));
    }
    let opened_identity = same_file::Handle::from_file(file.try_clone().map_err(|source| {
        TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }
    })?)
    .map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if opened_identity != initial_identity {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker changed while opening: {}",
            path.display(),
        )));
    }

    let record_len = usize::try_from(initial_metadata.len()).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "WAL publish boundary marker length does not fit memory: {}",
            path.display(),
        ))
    })?;
    let mut bytes = [0u8; PUBLISHED_HIGHWATER_MAX_RECORD_LEN];
    file.read_exact(&mut bytes[..record_len])
        .map_err(|source| {
            TsinkError::DataCorruption(format!(
                "WAL publish boundary marker changed length while reading at {}: {source}",
                path.display(),
            ))
        })?;
    let mut growth_probe = [0u8; 1];
    if file
        .read(&mut growth_probe)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?
        != 0
    {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker grew while reading: {}",
            path.display(),
        )));
    }

    let current_metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    require_plain_published_highwater_metadata(path, &current_metadata)?;
    if current_metadata.len() != initial_metadata.len() {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker changed length while reading: {}",
            path.display(),
        )));
    }
    let current_identity =
        same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if opened_identity != current_identity {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker changed while reading: {}",
            path.display(),
        )));
    }

    decode_published_highwater_record(&bytes[..record_len]).map(Some)
}

fn discard_unpublished_suffixes(
    segments: &[WalSegmentFile],
    published_record: PublishedHighwaterRecord,
    replay_highwater: WalHighWatermark,
) -> Result<()> {
    let published_highwater = published_record.highwater;
    let reset_highwater = published_record.reset_through;
    let effective_replay_highwater = replay_highwater.max(reset_highwater.unwrap_or_default());
    let reset_cleanup = reset_highwater == Some(published_highwater);
    struct TruncationPlan {
        segment_id: u64,
        path: PathBuf,
        file: File,
        current_len: u64,
        truncate_len: u64,
    }

    // Build and validate the complete plan before changing any segment. In particular, a corrupt
    // published frame in a later segment must not leave an earlier segment partially truncated.
    let mut plans = Vec::with_capacity(segments.len());
    let mut saw_boundary_segment = false;
    for segment in segments {
        let file = open_existing_wal_segment_with(&segment.path, |options| {
            options.read(true).write(true);
        })?;
        let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
            path: segment.path.clone(),
            source,
        })?;
        if !metadata.file_type().is_file() {
            return Err(TsinkError::DataCorruption(format!(
                "WAL segment is not a regular file during publish-boundary preflight: {}",
                segment.path.display()
            )));
        }
        let current_len = metadata.len();
        let validated_prefix_len = match segment.id.cmp(&published_highwater.segment) {
            std::cmp::Ordering::Less => {
                validate_published_segment_prefix(
                    &file,
                    &segment.path,
                    PublishedSegmentBoundary::EntireFile,
                )?;
                current_len
            }
            std::cmp::Ordering::Equal => {
                saw_boundary_segment = true;
                validate_published_segment_prefix(
                    &file,
                    &segment.path,
                    PublishedSegmentBoundary::Frame {
                        frame: published_highwater.frame,
                        allow_absent: published_highwater <= effective_replay_highwater,
                    },
                )?
            }
            // No byte in a segment after the published segment can be visible. Opening every
            // later path above is still part of preflight so a later access failure cannot occur
            // after an earlier truncation.
            std::cmp::Ordering::Greater => 0,
        };
        // When the two V2 boundaries are equal, finish an interrupted reset by clearing every
        // surviving WAL file after the complete namespace has passed preflight. After later
        // commits, files strictly before the retained reset floor remain safe to clear, while the
        // floor segment can contain newer frames and must retain its validated prefix.
        let truncate_len =
            if reset_cleanup || reset_highwater.is_some_and(|reset| segment.id < reset.segment) {
                0
            } else {
                validated_prefix_len
            };
        plans.push(TruncationPlan {
            segment_id: segment.id,
            path: segment.path.clone(),
            file,
            current_len,
            truncate_len,
        });
    }
    if !saw_boundary_segment {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary references missing segment {}",
            published_highwater.segment
        )));
    }

    for plan in plans {
        if plan.current_len <= plan.truncate_len {
            continue;
        }
        plan.file
            .set_len(plan.truncate_len)
            .and_then(|()| plan.file.sync_data())
            .map_err(|source| TsinkError::IoWithPath {
                path: plan.path.clone(),
                source,
            })?;
        warn!(
            segment = plan.segment_id,
            path = %plan.path.display(),
            published_segment = published_highwater.segment,
            published_frame = published_highwater.frame,
            discarded_bytes = plan.current_len.saturating_sub(plan.truncate_len),
            "WAL open discarded unpublished suffix"
        );
    }

    Ok(())
}

fn validate_markerless_published_segments(segments: &[WalSegmentFile]) -> Result<()> {
    for segment in segments {
        let file = open_existing_wal_segment_for_read(&segment.path)?;
        let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
            path: segment.path.clone(),
            source,
        })?;
        if !metadata.file_type().is_file() {
            return Err(TsinkError::DataCorruption(format!(
                "markerless WAL segment is not a regular file during published-prefix preflight: {}",
                segment.path.display()
            )));
        }
        validate_published_segment_prefix(
            &file,
            &segment.path,
            PublishedSegmentBoundary::EntireFile,
        )?;
    }
    Ok(())
}

fn validate_required_published_segment_coverage(
    segments: &[WalSegmentFile],
    replay_highwater: WalHighWatermark,
    published_highwater: WalHighWatermark,
) -> Result<()> {
    if published_highwater <= replay_highwater {
        return Ok(());
    }

    let mut required = segments.iter().filter(|segment| {
        segment.id >= replay_highwater.segment && segment.id <= published_highwater.segment
    });
    let first = required.next().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "WAL replay-required published interval {}:{}..={}:{} has no segment files",
            replay_highwater.segment,
            replay_highwater.frame,
            published_highwater.segment,
            published_highwater.frame,
        ))
    })?;
    if first.id != replay_highwater.segment {
        return Err(TsinkError::DataCorruption(format!(
            "WAL replay-required published interval is missing floor segment {} before segment {}",
            replay_highwater.segment, first.id,
        )));
    }

    let mut previous = first.id;
    for segment in required {
        let expected = previous.checked_add(1).ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "WAL segment id overflow after {previous} in replay-required published interval",
            ))
        })?;
        if segment.id != expected {
            return Err(TsinkError::DataCorruption(format!(
                "non-contiguous WAL segment namespace in replay-required published interval: segment {previous} is followed by {}",
                segment.id,
            )));
        }
        previous = segment.id;
    }
    if previous != published_highwater.segment {
        return Err(TsinkError::DataCorruption(format!(
            "WAL replay-required published interval ends at missing segment {} after segment {previous}",
            published_highwater.segment,
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum PublishedSegmentBoundary {
    EntireFile,
    Frame { frame: u64, allow_absent: bool },
}

/// Validates the complete published portion of one segment and returns its retained byte length.
///
/// A marker defines a prefix, not merely the latest frame to expose. Every retained frame is
/// checksummed and grammar-validated before any segment is truncated. An empty boundary segment,
/// or one whose first frame follows the marker, remains valid only when an authoritative replay
/// floor already covers the marker: WAL reset can then remove checkpointed frames while retaining
/// the monotonic high-watermark.
fn validate_published_segment_prefix(
    file: &File,
    path: &Path,
    boundary: PublishedSegmentBoundary,
) -> Result<u64> {
    let mut reader = BufReader::new(file.try_clone().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?);
    let mut prefix_len = 0u64;
    let mut previous_frame_seq = None;

    loop {
        let header = match read_header(&mut reader)? {
            HeaderRead::Eof => {
                return match boundary {
                    PublishedSegmentBoundary::EntireFile => Ok(prefix_len),
                    PublishedSegmentBoundary::Frame {
                        allow_absent: true, ..
                    } if previous_frame_seq.is_none() => Ok(0),
                    PublishedSegmentBoundary::Frame {
                        frame: published_frame,
                        ..
                    } => Err(TsinkError::DataCorruption(format!(
                        "WAL publish boundary frame {published_frame} is missing from {}",
                        path.display()
                    ))),
                };
            }
            HeaderRead::Truncated => {
                return Err(TsinkError::DataCorruption(format!(
                    "published WAL prefix has a truncated frame header: {}",
                    path.display()
                )))
            }
            HeaderRead::FrameHeader(header) => header,
        };

        let parsed_header = parse_frame_header(&header)?.ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "published WAL prefix has a frame magic mismatch: {}",
                path.display()
            ))
        })?;
        if parsed_header.frame_seq == 0
            || previous_frame_seq.is_some_and(|previous| parsed_header.frame_seq <= previous)
        {
            return Err(TsinkError::DataCorruption(format!(
                "published WAL prefix has a non-increasing frame sequence {} after {:?}: {}",
                parsed_header.frame_seq,
                previous_frame_seq,
                path.display()
            )));
        }
        if let PublishedSegmentBoundary::Frame {
            frame: published_frame,
            allow_absent,
        } = boundary
        {
            if parsed_header.frame_seq > published_frame {
                if allow_absent && previous_frame_seq.is_none() {
                    // Reset can leave an empty logical prefix followed by an abandoned append.
                    return Ok(0);
                }
                return Err(TsinkError::DataCorruption(format!(
                    "WAL publish boundary frame {published_frame} is missing before frame {} in {}",
                    parsed_header.frame_seq,
                    path.display()
                )));
            }
        }
        if parsed_header.payload_len > MAX_FRAME_PAYLOAD_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "published WAL frame {} exceeds the {}-byte payload limit: {}",
                parsed_header.frame_seq,
                MAX_FRAME_PAYLOAD_BYTES,
                path.display()
            )));
        }

        let mut payload = vec![0u8; parsed_header.payload_len];
        if let Err(source) = reader.read_exact(&mut payload) {
            return if source.kind() == std::io::ErrorKind::UnexpectedEof {
                Err(TsinkError::DataCorruption(format!(
                    "published WAL frame {} has a truncated payload: {}",
                    parsed_header.frame_seq,
                    path.display()
                )))
            } else {
                Err(TsinkError::IoWithPath {
                    path: path.to_path_buf(),
                    source,
                })
            };
        }
        if checksum32(&payload) != parsed_header.expected_crc32 {
            return Err(TsinkError::DataCorruption(format!(
                "published WAL frame {} has a checksum mismatch: {}",
                parsed_header.frame_seq,
                path.display()
            )));
        }
        validate_frame_payload_structure(parsed_header.frame_type, &payload).map_err(|err| {
            TsinkError::DataCorruption(format!(
                "published WAL frame {} has an invalid payload: {err}: {}",
                parsed_header.frame_seq,
                path.display()
            ))
        })?;

        previous_frame_seq = Some(parsed_header.frame_seq);
        prefix_len = prefix_len
            .checked_add(FRAME_HEADER_LEN as u64)
            .and_then(|bytes| bytes.checked_add(parsed_header.payload_len as u64))
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "published WAL prefix length overflow: {}",
                    path.display()
                ))
            })?;
        if matches!(
            boundary,
            PublishedSegmentBoundary::Frame {
                frame: published_frame,
                ..
            }
                if parsed_header.frame_seq == published_frame
        ) {
            return Ok(prefix_len);
        }
    }
}

pub(super) fn write_published_highwater_marker<F>(
    dir: &Path,
    path: &Path,
    tmp_path: &Path,
    record: PublishedHighwaterRecord,
    sync: bool,
    post_rename: F,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    // `wal.published.tmp` is an owned recovery name. Never open an existing entry: it could be a
    // symlink or a hard link to a WAL segment. Unlinking the directory entry does not follow either
    // kind of link, and `create_new` below closes the replacement race. A real directory remains a
    // hard failure rather than being recursively removed.
    match fs::symlink_metadata(tmp_path) {
        Ok(metadata)
            if metadata.file_type().is_dir()
                && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
        {
            return Err(TsinkError::DataCorruption(format!(
                "WAL publish boundary temporary path is a directory: {}",
                tmp_path.display(),
            )));
        }
        Ok(_) => {
            crate::engine::fs_utils::remove_file_if_exists(tmp_path).map_err(|source| {
                TsinkError::IoWithPath {
                    path: tmp_path.to_path_buf(),
                    source,
                }
            })?;
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: tmp_path.to_path_buf(),
                source,
            })
        }
    }

    let bytes = encode_published_highwater_record(record);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_wal_no_follow(&mut options);
    // If writing or syncing this exclusively created file fails, leave it at the owned temporary
    // name. The next attempt applies the stale-entry policy above. Avoid a best-effort cleanup here:
    // a racing replacement must never be unlinked on the assumption that it is still our file.
    let mut file = options.open(tmp_path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::AlreadyExists {
            TsinkError::DataCorruption(format!(
                "WAL publish boundary temporary path appeared while creating it: {}",
                tmp_path.display(),
            ))
        } else {
            TsinkError::IoWithPath {
                path: tmp_path.to_path_buf(),
                source,
            }
        }
    })?;
    file.write_all(&bytes)
        .map_err(|source| TsinkError::IoWithPath {
            path: tmp_path.to_path_buf(),
            source,
        })?;
    if sync {
        file.sync_data().map_err(|source| TsinkError::IoWithPath {
            path: tmp_path.to_path_buf(),
            source,
        })?;
    }

    let opened_metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: tmp_path.to_path_buf(),
        source,
    })?;
    require_plain_published_highwater_metadata(tmp_path, &opened_metadata)?;
    if opened_metadata.len() != bytes.len() as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary temporary file changed length after writing: {}",
            tmp_path.display(),
        )));
    }
    let opened_identity = same_file::Handle::from_file(file.try_clone().map_err(|source| {
        TsinkError::IoWithPath {
            path: tmp_path.to_path_buf(),
            source,
        }
    })?)
    .map_err(|source| TsinkError::IoWithPath {
        path: tmp_path.to_path_buf(),
        source,
    })?;
    let current_metadata =
        fs::symlink_metadata(tmp_path).map_err(|source| TsinkError::IoWithPath {
            path: tmp_path.to_path_buf(),
            source,
        })?;
    require_plain_published_highwater_metadata(tmp_path, &current_metadata)?;
    if current_metadata.len() != bytes.len() as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary temporary path changed length before replacement: {}",
            tmp_path.display(),
        )));
    }
    let current_identity =
        same_file::Handle::from_path(tmp_path).map_err(|source| TsinkError::IoWithPath {
            path: tmp_path.to_path_buf(),
            source,
        })?;
    if opened_identity != current_identity {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary temporary path changed before replacement: {}",
            tmp_path.display(),
        )));
    }
    drop(opened_identity);
    drop(current_identity);
    drop(file);

    crate::engine::fs_utils::rename_tmp(tmp_path, path)?;
    let replaced = read_published_highwater(path)?.ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "WAL publish boundary marker disappeared after replacement: {}",
            path.display(),
        ))
    })?;
    if replaced != record {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary marker changed during replacement: expected {record:?}, found {replaced:?} at {}",
            path.display(),
        )));
    }
    post_rename()?;
    if sync {
        sync_dir_path(dir)?;
    }
    Ok(())
}

#[cfg(not(windows))]
pub(super) fn sync_dir_path(path: &Path) -> Result<()> {
    let dir = File::open(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(windows)]
pub(super) fn sync_dir_path(_path: &Path) -> Result<()> {
    // Windows does not support flushing directory handles directly.
    Ok(())
}

pub(super) fn collect_wal_segment_files(dir: &Path) -> Result<Vec<WalSegmentFile>> {
    collect_wal_segment_files_with_limit(
        dir,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
    )
}

fn collect_wal_segment_files_with_limit(
    dir: &Path,
    max_entries: usize,
) -> Result<Vec<WalSegmentFile>> {
    let mut deduped = BTreeMap::<u64, WalSegmentFile>::new();
    let entries = crate::engine::fs_utils::collect_directory_entries_bounded(
        dir,
        max_entries,
        "WAL segment discovery",
    )?;
    for entry in entries {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let segment_id = if file_name == WAL_FILE_NAME {
            Some(0)
        } else {
            parse_segment_file_name(&file_name)
        };

        let Some(segment_id) = segment_id else {
            continue;
        };
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "recognized WAL segment must be a regular non-link file: {}",
                path.display(),
            )));
        }
        if let Some(existing) = deduped.get(&segment_id) {
            return Err(TsinkError::DataCorruption(format!(
                "duplicate WAL segment id {segment_id} is represented by both {} and {}",
                existing.path.display(),
                path.display(),
            )));
        }
        deduped.insert(
            segment_id,
            WalSegmentFile {
                id: segment_id,
                path,
            },
        );
    }

    Ok(deduped.into_values().collect())
}

fn scan_wal_runtime_accounting(dir: &Path) -> Result<WalRuntimeAccounting> {
    let segments = collect_wal_segment_files(dir)?;
    WalRuntimeAccounting::from_segments(&segments)
}

fn configure_wal_no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
}

fn require_plain_wal_segment_metadata(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if crate::engine::fs_utils::is_link_or_reparse_point(metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "WAL segment must be a regular non-link file: {}",
            path.display(),
        )));
    }
    Ok(())
}

fn open_existing_wal_segment_with(
    path: &Path,
    configure: impl FnOnce(&mut OpenOptions),
) -> Result<File> {
    let initial_metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    require_plain_wal_segment_metadata(path, &initial_metadata)?;
    let initial_identity =
        same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;

    let mut options = OpenOptions::new();
    configure(&mut options);
    configure_wal_no_follow(&mut options);
    let file = options
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let opened_metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    require_plain_wal_segment_metadata(path, &opened_metadata)?;
    let opened_identity = same_file::Handle::from_file(file.try_clone().map_err(|source| {
        TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }
    })?)
    .map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if opened_identity != initial_identity {
        return Err(TsinkError::DataCorruption(format!(
            "WAL segment path changed while opening: {}",
            path.display(),
        )));
    }

    let current_metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    require_plain_wal_segment_metadata(path, &current_metadata)?;
    let current_identity =
        same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if opened_identity != current_identity {
        return Err(TsinkError::DataCorruption(format!(
            "WAL segment path changed after opening: {}",
            path.display(),
        )));
    }

    Ok(file)
}

pub(super) fn open_existing_wal_segment_for_read(path: &Path) -> Result<File> {
    open_existing_wal_segment_with(path, |options| {
        options.read(true);
    })
}

pub(super) fn open_segment_for_append(path: &Path) -> Result<(File, bool, u64)> {
    let (file, segment_created) = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            require_plain_wal_segment_metadata(path, &metadata)?;
            (
                open_existing_wal_segment_with(path, |options| {
                    options.append(true);
                })?,
                false,
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut options = OpenOptions::new();
            options.append(true).create_new(true);
            configure_wal_no_follow(&mut options);
            let file = options.open(path).map_err(|source| {
                if source.kind() == std::io::ErrorKind::AlreadyExists {
                    TsinkError::DataCorruption(format!(
                        "WAL segment path appeared while creating it: {}",
                        path.display(),
                    ))
                } else {
                    TsinkError::IoWithPath {
                        path: path.to_path_buf(),
                        source,
                    }
                }
            })?;
            let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?;
            require_plain_wal_segment_metadata(path, &metadata)?;
            (file, true)
        }
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let initial_len = file.metadata()?.len();
    Ok((file, segment_created, initial_len))
}

fn scan_segments_for_open(segments: &[WalSegmentFile]) -> Result<WalOpenRecoveryState> {
    let active_segment = segments
        .last()
        .map(|segment| segment.id)
        .unwrap_or_default();
    let mut recovery = WalOpenRecoveryState::default();

    for segment in segments {
        let scan = scan_recoverable_segment(&segment.path)?;
        if scan.max_seq > 0 {
            recovery.last_highwater = WalHighWatermark {
                segment: segment.id,
                frame: scan.max_seq,
            };
        }
        if segment.id == active_segment {
            recovery.active_segment_last_seq = scan.max_seq;
            recovery.quarantine_active_segment = scan.encountered_corruption;
        }
    }

    Ok(recovery)
}

fn parse_segment_file_name(file_name: &str) -> Option<u64> {
    if !file_name.starts_with(WAL_SEGMENT_FILE_PREFIX)
        || !file_name.ends_with(WAL_SEGMENT_FILE_SUFFIX)
    {
        return None;
    }

    let hex_start = WAL_SEGMENT_FILE_PREFIX.len();
    let hex_end = file_name
        .len()
        .saturating_sub(WAL_SEGMENT_FILE_SUFFIX.len());
    if hex_end <= hex_start {
        return None;
    }

    let hex = &file_name[hex_start..hex_end];
    if hex.len() != 16 {
        return None;
    }

    u64::from_str_radix(hex, 16).ok()
}

/// Establishes a manifestless directory's identity from the exact, persisted framed-WAL
/// namespace without opening the WAL for recovery. This deliberately performs no creation,
/// truncation, quarantine, rename, or cleanup.
///
/// A non-empty WAL is accepted only after every segment's first frame has a canonical current
/// header and one deterministic candidate frame has passed its payload checksum and codec
/// decoder. The exact one-file, zero-length namespace is also the durable bootstrap/fully
/// rolled-back state produced before the first successful append; no arbitrary bytes are
/// accepted as that state.
pub(in crate::engine) fn validate_legacy_wal_identity(
    dir: &Path,
    startup_memory_budget: usize,
) -> Result<bool> {
    let dir_metadata = match fs::symlink_metadata(dir) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: dir.to_path_buf(),
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&dir_metadata)
        || !dir_metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL identity root is link-like or not a directory: {}",
            dir.display()
        )));
    }
    let initial_dir_identity =
        same_file::Handle::from_path(dir).map_err(|source| TsinkError::IoWithPath {
            path: dir.to_path_buf(),
            source,
        })?;

    let mut observed_entries = 0usize;
    let mut saw_segment = false;
    let mut saw_legacy_zero_alias = false;
    let mut saw_canonical_zero = false;
    let mut saw_published_marker = false;
    let mut nonempty_candidate: Option<(u64, PathBuf, u64)> = None;

    for entry in fs::read_dir(dir).map_err(|source| TsinkError::IoWithPath {
        path: dir.to_path_buf(),
        source,
    })? {
        observed_entries = observed_entries.checked_add(1).ok_or_else(|| {
            TsinkError::Other("legacy WAL namespace entry counter overflow".to_string())
        })?;
        if observed_entries > crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES {
            return Err(TsinkError::DataCorruption(format!(
                "legacy WAL identity exceeds its {}-entry work bound: {}",
                crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
                dir.display()
            )));
        }
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "legacy WAL contains a non-UTF-8 entry: {}",
                path.display()
            ))
        })?;
        let metadata = fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "legacy WAL entry is link-like or not a regular file: {}",
                path.display()
            )));
        }

        if matches!(
            name,
            WAL_PUBLISHED_HIGHWATER_FILE_NAME | WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME
        ) {
            validate_legacy_published_marker(&path, metadata.len())?;
            if name == WAL_PUBLISHED_HIGHWATER_FILE_NAME {
                saw_published_marker = true;
            }
            continue;
        }

        let segment_id = if name == WAL_FILE_NAME {
            saw_legacy_zero_alias = true;
            Some(0)
        } else {
            parse_segment_file_name(name).filter(|segment_id| {
                segment_path(dir, *segment_id)
                    .file_name()
                    .and_then(|candidate| candidate.to_str())
                    == Some(name)
            })
        }
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "legacy WAL contains an unknown or non-canonical entry: {}",
                path.display()
            ))
        })?;
        if segment_id == 0 && name != WAL_FILE_NAME {
            saw_canonical_zero = true;
        }
        if saw_legacy_zero_alias && saw_canonical_zero {
            return Err(TsinkError::DataCorruption(format!(
                "legacy WAL contains duplicate segment-zero aliases: {}",
                dir.display()
            )));
        }

        saw_segment = true;
        if metadata.len() == 0 {
            continue;
        }
        let parsed = probe_legacy_wal_frame(&path, metadata.len())?;
        if nonempty_candidate
            .as_ref()
            .is_none_or(|(candidate_id, _, _)| segment_id < *candidate_id)
        {
            nonempty_candidate = Some((segment_id, path, metadata.len()));
        }
        // Parse here, before candidate selection, so arbitrary bytes in any named segment cannot
        // be hidden behind a different valid segment.
        let _ = parsed;
    }

    if !saw_segment {
        return Ok(false);
    }
    if let Some((_, path, file_len)) = nonempty_candidate {
        validate_legacy_wal_frame_payload(&path, file_len, startup_memory_budget)?;
    } else if !saw_published_marker {
        return Err(TsinkError::DataCorruption(format!(
            "legacy empty WAL bootstrap identity requires a checksummed publish marker: {}",
            dir.display()
        )));
    }

    let current_dir_metadata =
        fs::symlink_metadata(dir).map_err(|source| TsinkError::IoWithPath {
            path: dir.to_path_buf(),
            source,
        })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&current_dir_metadata)
        || !current_dir_metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL identity root changed type while validating: {}",
            dir.display()
        )));
    }
    let current_dir_identity =
        same_file::Handle::from_path(dir).map_err(|source| TsinkError::IoWithPath {
            path: dir.to_path_buf(),
            source,
        })?;
    if initial_dir_identity != current_dir_identity {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL identity root changed while validating: {}",
            dir.display()
        )));
    }

    Ok(true)
}

fn validate_legacy_published_marker(path: &Path, file_len: u64) -> Result<()> {
    if file_len != PUBLISHED_HIGHWATER_RECORD_LEN as u64
        && file_len != PUBLISHED_HIGHWATER_V2_RECORD_LEN as u64
    {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL publish marker has length {file_len}, expected {PUBLISHED_HIGHWATER_RECORD_LEN} or {PUBLISHED_HIGHWATER_V2_RECORD_LEN}: {}",
            path.display()
        )));
    }
    let mut file = open_legacy_wal_file_no_follow(path, file_len)?;
    let record_len = usize::try_from(file_len).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "legacy WAL publish marker length does not fit memory: {}",
            path.display()
        ))
    })?;
    let mut bytes = [0u8; PUBLISHED_HIGHWATER_MAX_RECORD_LEN];
    file.read_exact(&mut bytes[..record_len])
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    decode_published_highwater_record(&bytes[..record_len]).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "legacy WAL publish marker failed read-only identity validation at {}: {err}",
            path.display()
        ))
    })?;
    validate_opened_legacy_wal_file_identity(file, path, file_len)
}

fn probe_legacy_wal_frame(path: &Path, file_len: u64) -> Result<ParsedFrameHeader> {
    let mut file = open_legacy_wal_file_no_follow(path, file_len)?;
    let mut header = [0u8; FRAME_HEADER_LEN];
    file.read_exact(&mut header).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "legacy WAL segment has a truncated first frame at {}: {err}",
            path.display()
        ))
    })?;
    let parsed = parse_frame_header(&header)
        .map_err(|err| {
            TsinkError::DataCorruption(format!(
                "legacy WAL segment first frame is malformed at {}: {err}",
                path.display()
            ))
        })?
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "legacy WAL segment first frame magic is invalid: {}",
                path.display()
            ))
        })?;
    if header[5..8] != [0u8; 3] {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL segment first frame has nonzero reserved header bytes: {}",
            path.display()
        )));
    }
    if parsed.frame_seq == 0 {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL segment first frame sequence is zero: {}",
            path.display()
        )));
    }
    if !matches!(
        parsed.frame_type,
        FRAME_TYPE_SERIES_DEF | FRAME_TYPE_SAMPLES
    ) {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL segment first frame type {} is unknown: {}",
            parsed.frame_type,
            path.display()
        )));
    }
    if parsed.payload_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL segment first frame payload {} exceeds the {}-byte format limit: {}",
            parsed.payload_len,
            MAX_FRAME_PAYLOAD_BYTES,
            path.display()
        )));
    }
    let required_len = (FRAME_HEADER_LEN as u64)
        .checked_add(parsed.payload_len as u64)
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "legacy WAL segment first frame length overflows: {}",
                path.display()
            ))
        })?;
    if required_len > file_len {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL segment first frame is truncated: {}",
            path.display()
        )));
    }
    validate_opened_legacy_wal_file_identity(file, path, file_len)?;
    Ok(parsed)
}

fn validate_legacy_wal_frame_payload(
    path: &Path,
    file_len: u64,
    startup_memory_budget: usize,
) -> Result<()> {
    let parsed = probe_legacy_wal_frame(path, file_len)?;
    let required = parsed
        .payload_len
        .checked_add(LEGACY_WAL_IDENTITY_FIXED_MEMORY_BYTES)
        .ok_or_else(|| {
            TsinkError::Other("legacy WAL identity memory model overflow".to_string())
        })?;
    crate::disk_budget::admit_startup_memory(startup_memory_budget, required)?;

    let mut file = open_legacy_wal_file_no_follow(path, file_len)?;
    let mut header = [0u8; FRAME_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(parsed.payload_len)
        .map_err(|err| {
            TsinkError::Other(format!(
                "unable to allocate {} bytes for legacy WAL identity validation at {}: {err}",
                parsed.payload_len,
                path.display()
            ))
        })?;
    payload.resize(parsed.payload_len, 0);
    file.read_exact(&mut payload).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "legacy WAL segment first-frame payload became truncated at {}: {err}",
            path.display()
        ))
    })?;
    if checksum32(&payload) != parsed.expected_crc32 {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL segment first-frame checksum mismatch: {}",
            path.display()
        )));
    }
    validate_frame_payload_structure(parsed.frame_type, &payload).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "legacy WAL segment first-frame payload failed codec validation at {}: {err}",
            path.display()
        ))
    })?;
    validate_opened_legacy_wal_file_identity(file, path, file_len)
}

fn open_legacy_wal_file_no_follow(path: &Path, expected_len: u64) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() != expected_len
    {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL file changed type or length while opening: {}",
            path.display()
        )));
    }
    Ok(file)
}

fn validate_opened_legacy_wal_file_identity(
    file: File,
    path: &Path,
    expected_len: u64,
) -> Result<()> {
    let opened_identity =
        same_file::Handle::from_file(file).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let current_metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&current_metadata)
        || !current_metadata.file_type().is_file()
        || current_metadata.len() != expected_len
    {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL file changed type or length while validating: {}",
            path.display()
        )));
    }
    let current_identity =
        same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    if opened_identity != current_identity {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL file path changed while validating: {}",
            path.display()
        )));
    }
    Ok(())
}

pub(super) fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!(
        "{WAL_SEGMENT_FILE_PREFIX}{segment_id:016x}{WAL_SEGMENT_FILE_SUFFIX}"
    ))
}

fn scan_recoverable_segment(path: &Path) -> Result<RecoverableSegmentScan> {
    let file = open_existing_wal_segment_for_read(path)?;
    let mut reader = BufReader::new(file);
    let mut max_seq = 0u64;
    let mut encountered_corruption = false;

    loop {
        let header = match read_header(&mut reader)? {
            HeaderRead::Eof => break,
            HeaderRead::Truncated => {
                encountered_corruption = true;
                break;
            }
            HeaderRead::FrameHeader(header) => header,
        };

        let Some(parsed_header) = parse_frame_header(&header)? else {
            encountered_corruption = true;
            break;
        };
        let frame_type = parsed_header.frame_type;
        let frame_seq = parsed_header.frame_seq;
        let payload_len = parsed_header.payload_len;
        let expected_crc32 = parsed_header.expected_crc32;

        if payload_len > MAX_FRAME_PAYLOAD_BYTES {
            encountered_corruption = true;
            break;
        }

        let mut payload = vec![0u8; payload_len];
        if let Err(err) = reader.read_exact(&mut payload) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                encountered_corruption = true;
                break;
            }
            return Err(err.into());
        }

        if checksum32(&payload) != expected_crc32 {
            encountered_corruption = true;
            continue;
        }

        let decoded = match frame_type {
            FRAME_TYPE_SERIES_DEF => decode_series_definition(&payload).map(|_| ()),
            FRAME_TYPE_SAMPLES => decode_samples_payload(&payload).map(|_| ()),
            _ => Err(TsinkError::DataCorruption(
                "unknown WAL frame type".to_string(),
            )),
        };
        if decoded.is_err() {
            encountered_corruption = true;
            continue;
        }

        max_seq = max_seq.max(frame_seq);
    }

    Ok(RecoverableSegmentScan {
        max_seq,
        encountered_corruption,
    })
}

#[cfg(test)]
pub(super) fn scan_last_seq(path: &Path) -> Result<u64> {
    let file = open_existing_wal_segment_for_read(path)?;
    let mut reader = BufReader::new(file);
    let mut last_seq = 0u64;
    let mut payload = Vec::new();

    loop {
        let mut header = [0u8; FRAME_HEADER_LEN];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let Some(parsed_header) = parse_frame_header(&header)? else {
            break;
        };
        let frame_seq = parsed_header.frame_seq;
        let payload_len = parsed_header.payload_len;
        let expected_crc32 = parsed_header.expected_crc32;

        if payload_len > MAX_FRAME_PAYLOAD_BYTES {
            break;
        }

        payload.resize(payload_len, 0);
        if reader.read_exact(payload.as_mut_slice()).is_err() {
            break;
        }

        if checksum32(payload.as_slice()) != expected_crc32 {
            break;
        }

        last_seq = last_seq.max(frame_seq);
    }

    Ok(last_seq)
}

#[cfg(test)]
mod namespace_bound_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn wal_segment_discovery_counts_unknown_entries_at_the_global_cap() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join(WAL_FILE_NAME), b"wal").unwrap();
        fs::write(temp_dir.path().join("host-owned"), b"opaque").unwrap();

        let segments = collect_wal_segment_files_with_limit(temp_dir.path(), 2)
            .expect("the exact namespace cap must succeed");
        assert_eq!(segments.len(), 1);

        fs::create_dir(temp_dir.path().join("unknown-directory")).unwrap();
        let err = collect_wal_segment_files_with_limit(temp_dir.path(), 2)
            .expect_err("cap plus one must fail before name filtering");
        assert!(err.to_string().contains("2-entry global work bound"));
    }
}
