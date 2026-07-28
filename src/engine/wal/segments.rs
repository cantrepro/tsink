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
        )
    }

    pub(in crate::engine) fn open_with_buffer_size_and_disk_budget_and_replay_mode_and_creation(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
        replay_mode: WalReplayMode,
        allow_namespace_creation: bool,
    ) -> Result<Self> {
        Self::open_with_options_and_disk_budget(
            dir,
            sync_mode,
            buffer_size,
            DEFAULT_WAL_SEGMENT_MAX_BYTES,
            local_disk_budget,
            replay_mode,
            allow_namespace_creation,
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
            sync_mode,
            buffer_size,
            segment_max_bytes,
            None,
            WalReplayMode::Strict,
            true,
        )
    }

    fn open_with_options_and_disk_budget(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        segment_max_bytes: u64,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
        replay_mode: WalReplayMode,
        allow_namespace_creation: bool,
    ) -> Result<Self> {
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
            File::create(&path)?;
            sync_dir_path(&dir)?;
            segments.push(WalSegmentFile { id: 0, path });
        }

        let mut active = segments.last().cloned().ok_or_else(|| TsinkError::Wal {
            operation: "open".to_string(),
            details: "missing WAL segment after initialization".to_string(),
        })?;
        let published_highwater_path = published_highwater_path(&dir);
        let published_highwater_tmp_path = published_highwater_tmp_path(&dir);
        let existing_published_highwater = read_published_highwater(&published_highwater_path)?;
        if existing_published_highwater.is_none() && !allow_namespace_creation {
            return Err(TsinkError::DataCorruption(format!(
                "non-creating WAL open requires the canonical publish-boundary marker: {}",
                published_highwater_path.display()
            )));
        }
        if let Some(published_highwater) = existing_published_highwater {
            discard_unpublished_suffixes(&segments, published_highwater)?;
        }
        let recovery = scan_segments_for_open(&segments)?;
        let mut active_last_seq = recovery.active_segment_last_seq;
        let last_highwater = recovery.last_highwater;
        if recovery.quarantine_active_segment && replay_mode == WalReplayMode::Strict {
            return Err(TsinkError::DataCorruption(format!(
                "strict WAL open detected corruption in active segment {} at {}",
                active.id,
                active.path.display()
            )));
        }

        let published_highwater = existing_published_highwater.unwrap_or(last_highwater);
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

        if existing_published_highwater.is_none() {
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
                    PUBLISHED_HIGHWATER_RECORD_LEN as u64,
                    crate::DiskReservationKind::Recovery,
                )
            })
            .transpose()?;
        let reset_highwater = self.current_appended_highwater();
        let reset_result = (|| -> Result<()> {
            writer.flush()?;
            writer.get_mut().sync_data()?;
            let active_path = self.path.lock().clone();
            let replacement = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&active_path)?;
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
            self.persist_published_highwater(reset_highwater, true)?;
            Ok(())
        })();

        if reset_result.is_ok() {
            // Publish the exact post-clear cache charge while the cache mutex is still held.
            // Growth observations use the same mutex, so an older observation cannot re-add a
            // stale charge after this reset releases it. Do this before settlement/reconciliation:
            // those later stages can fail after the cache was already cleared.
            self.clear_cached_series_definition_index_if_initialized(observe_reset_cache);
            self.mark_published_through(reset_highwater);
            self.mark_durable_through(reset_highwater);
            *self.last_sync.lock() = Instant::now();
        }

        // Conservatively charge the complete marker until the exclusive scan below replaces all
        // WAL accounting with the exact post-reset tree. Settling first is required because an
        // active reservation would prevent idle reconciliation from beginning.
        let settlement_result = match disk_reservation.take() {
            Some(reservation) => reservation.commit(PUBLISHED_HIGHWATER_RECORD_LEN as u64, 0),
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

fn encode_published_highwater(highwater: WalHighWatermark) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PUBLISHED_HIGHWATER_RECORD_LEN);
    bytes.extend_from_slice(&PUBLISHED_HIGHWATER_MAGIC);
    append_u64(&mut bytes, highwater.segment);
    append_u64(&mut bytes, highwater.frame);
    let checksum = checksum32(&bytes);
    append_u32(&mut bytes, checksum);
    bytes
}

fn decode_published_highwater(bytes: &[u8]) -> Result<WalHighWatermark> {
    if bytes.len() != PUBLISHED_HIGHWATER_RECORD_LEN {
        return Err(TsinkError::DataCorruption(format!(
            "WAL publish boundary record must be {PUBLISHED_HIGHWATER_RECORD_LEN} bytes, found {}",
            bytes.len()
        )));
    }
    if bytes[0..4] != PUBLISHED_HIGHWATER_MAGIC {
        return Err(TsinkError::DataCorruption(
            "WAL publish boundary marker has an invalid magic header".to_string(),
        ));
    }

    let expected_checksum = read_u32_at(bytes, PUBLISHED_HIGHWATER_RECORD_LEN - 4)?;
    let actual_checksum = checksum32(&bytes[..PUBLISHED_HIGHWATER_RECORD_LEN - 4]);
    if expected_checksum != actual_checksum {
        return Err(TsinkError::DataCorruption(
            "WAL publish boundary marker checksum mismatch".to_string(),
        ));
    }

    Ok(WalHighWatermark {
        segment: read_u64_at(bytes, 4)?,
        frame: read_u64_at(bytes, 12)?,
    })
}

fn read_published_highwater(path: &Path) -> Result<Option<WalHighWatermark>> {
    match fs::read(path) {
        Ok(bytes) => decode_published_highwater(&bytes).map(Some),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn discard_unpublished_suffixes(
    segments: &[WalSegmentFile],
    published_highwater: WalHighWatermark,
) -> Result<()> {
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment.path)
            .map_err(|source| TsinkError::IoWithPath {
                path: segment.path.clone(),
                source,
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
        let truncate_len = match segment.id.cmp(&published_highwater.segment) {
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
                    PublishedSegmentBoundary::Frame(published_highwater.frame),
                )?
            }
            // No byte in a segment after the published segment can be visible. Opening every
            // later path above is still part of preflight so a later access failure cannot occur
            // after an earlier truncation.
            std::cmp::Ordering::Greater => 0,
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

#[derive(Debug, Clone, Copy)]
enum PublishedSegmentBoundary {
    EntireFile,
    Frame(u64),
}

/// Validates the complete published portion of one segment and returns its retained byte length.
///
/// A marker defines a prefix, not merely the latest frame to expose. Every retained frame is
/// checksummed and grammar-validated before any segment is truncated. An empty boundary segment
/// remains valid because WAL reset removes checkpointed frames while retaining the monotonic
/// high-watermark.
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
                    PublishedSegmentBoundary::Frame(_) if previous_frame_seq.is_none() => Ok(0),
                    PublishedSegmentBoundary::Frame(published_frame) => {
                        Err(TsinkError::DataCorruption(format!(
                            "WAL publish boundary frame {published_frame} is missing from {}",
                            path.display()
                        )))
                    }
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
        if let PublishedSegmentBoundary::Frame(published_frame) = boundary {
            if parsed_header.frame_seq > published_frame {
                if previous_frame_seq.is_none() {
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
            PublishedSegmentBoundary::Frame(published_frame)
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
    highwater: WalHighWatermark,
    sync: bool,
    post_rename: F,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    let write_result = (|| -> Result<()> {
        let bytes = encode_published_highwater(highwater);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)?;
        file.write_all(&bytes)?;
        if sync {
            file.sync_data()?;
        }
        drop(file);
        crate::engine::fs_utils::rename_tmp(tmp_path, path)?;
        post_rename()?;
        if sync {
            sync_dir_path(dir)?;
        }
        Ok(())
    })();

    if write_result.is_err() {
        let _ = crate::engine::fs_utils::remove_file_if_exists(tmp_path);
    }

    write_result
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
        if !entry.file_type()?.is_file() {
            continue;
        }

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
        let is_segment_file = file_name.starts_with(WAL_SEGMENT_FILE_PREFIX);
        deduped
            .entry(segment_id)
            .and_modify(|existing| {
                let existing_name = existing
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                if is_segment_file || existing_name == WAL_FILE_NAME {
                    *existing = WalSegmentFile {
                        id: segment_id,
                        path: path.clone(),
                    };
                }
            })
            .or_insert(WalSegmentFile {
                id: segment_id,
                path,
            });
    }

    Ok(deduped.into_values().collect())
}

fn scan_wal_runtime_accounting(dir: &Path) -> Result<WalRuntimeAccounting> {
    let segments = collect_wal_segment_files(dir)?;
    WalRuntimeAccounting::from_segments(&segments)
}

pub(super) fn open_segment_for_append(path: &Path) -> Result<(File, bool, u64)> {
    let segment_created = match fs::metadata(path) {
        Ok(_) => false,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => return Err(e.into()),
    };
    let file = OpenOptions::new().create(true).append(true).open(path)?;
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
    if file_len != PUBLISHED_HIGHWATER_RECORD_LEN as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "legacy WAL publish marker has length {file_len}, expected {PUBLISHED_HIGHWATER_RECORD_LEN}: {}",
            path.display()
        )));
    }
    let mut file = open_legacy_wal_file_no_follow(path, file_len)?;
    let mut bytes = [0u8; PUBLISHED_HIGHWATER_RECORD_LEN];
    file.read_exact(&mut bytes)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    decode_published_highwater(&bytes).map_err(|err| {
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
    let file = OpenOptions::new().read(true).open(path)?;
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
    let file = OpenOptions::new().read(true).open(path)?;
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
