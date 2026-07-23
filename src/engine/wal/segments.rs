use super::codec::{decode_samples_payload, decode_series_definition};
use super::replay::{parse_frame_header, read_header, HeaderRead};
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

    pub(in crate::engine) fn open_with_buffer_size_and_disk_budget(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Result<Self> {
        Self::open_with_options_and_disk_budget(
            dir,
            sync_mode,
            buffer_size,
            DEFAULT_WAL_SEGMENT_MAX_BYTES,
            local_disk_budget,
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
        )
    }

    fn open_with_options_and_disk_budget(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
        segment_max_bytes: u64,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut segments = collect_wal_segment_files(&dir)?;
        if segments.is_empty() {
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
        if let Some(published_highwater) = existing_published_highwater {
            discard_unpublished_suffixes(&segments, published_highwater)?;
        }
        let recovery = scan_segments_for_open(&segments)?;
        let mut active_last_seq = recovery.active_segment_last_seq;
        let last_highwater = recovery.last_highwater;

        let published_highwater = existing_published_highwater.unwrap_or(last_highwater);
        let writer_file = if recovery.quarantine_active_segment {
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
            configured_replay_mode: Mutex::new(WalReplayMode::Strict),
            sync_mode,
            last_sync: Mutex::new(Instant::now()),
            segment_max_bytes: segment_max_bytes.max(1),
            local_disk_budget,
            #[cfg(test)]
            append_sync_hook: Mutex::new(None),
            #[cfg(test)]
            cached_series_definition_rebuild_hook: Mutex::new(None),
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

    fn reset_locked(&self, mut writer: MutexGuard<'_, BufWriter<File>>) -> Result<()> {
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
            self.clear_cached_series_definition_index_if_initialized();
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
        self.reset_locked(self.writer.lock())
    }

    pub(crate) fn reset_if_current_highwater_at_most(
        &self,
        max_highwater: WalHighWatermark,
    ) -> Result<bool> {
        let writer = self.writer.lock();
        if *self.last_appended_highwater.lock() > max_highwater {
            return Ok(false);
        }

        self.reset_locked(writer)?;
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
        let mut writer = self.writer.lock();
        let mut active_segment = self.active_segment.load(Ordering::SeqCst);
        let mut next_seq = self.next_seq.load(Ordering::SeqCst);

        if active_segment < min_highwater.segment {
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
    for segment in segments {
        let truncate_len = match segment.id.cmp(&published_highwater.segment) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Equal => {
                published_prefix_len(&segment.path, published_highwater.frame)?
            }
            std::cmp::Ordering::Greater => Some(0),
        };

        let Some(truncate_len) = truncate_len else {
            continue;
        };
        let current_len = match fs::metadata(&segment.path) {
            Ok(metadata) => metadata.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if current_len <= truncate_len {
            continue;
        }

        let file = OpenOptions::new().write(true).open(&segment.path)?;
        file.set_len(truncate_len)?;
        file.sync_data()?;
        warn!(
            segment = segment.id,
            path = %segment.path.display(),
            published_segment = published_highwater.segment,
            published_frame = published_highwater.frame,
            discarded_bytes = current_len.saturating_sub(truncate_len),
            "WAL open discarded unpublished suffix"
        );
    }

    Ok(())
}

/// Returns the byte length of the physical prefix ending at `published_frame`.
///
/// A publish marker defines a prefix, not merely the latest frame to expose. Keeping
/// later frames around would let a future publish marker retroactively commit a write
/// that crashed before publication. An empty file is valid because WAL reset removes
/// checkpointed frames while retaining their monotonic high-watermark.
fn published_prefix_len(path: &Path, published_frame: u64) -> Result<Option<u64>> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut reader = BufReader::new(file);
    let mut prefix_len = 0u64;
    let mut saw_frame_at_or_before_boundary = false;

    loop {
        let header = match read_header(&mut reader)? {
            HeaderRead::Eof => {
                if !saw_frame_at_or_before_boundary {
                    return Ok(Some(0));
                }
                return Err(TsinkError::DataCorruption(format!(
                    "WAL publish boundary frame {published_frame} is missing from {}",
                    path.display()
                )));
            }
            HeaderRead::Truncated => return Ok(None),
            HeaderRead::FrameHeader(header) => header,
        };

        let Some(parsed_header) = parse_frame_header(&header)? else {
            return Ok(None);
        };
        if parsed_header.frame_seq > published_frame {
            if saw_frame_at_or_before_boundary {
                return Err(TsinkError::DataCorruption(format!(
                    "WAL publish boundary frame {published_frame} is missing before frame {} in {}",
                    parsed_header.frame_seq,
                    path.display()
                )));
            }
            return Ok(Some(0));
        }
        if parsed_header.payload_len > MAX_FRAME_PAYLOAD_BYTES {
            return Ok(None);
        }

        let mut payload = vec![0u8; parsed_header.payload_len];
        if let Err(err) = reader.read_exact(&mut payload) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(err.into());
        }

        saw_frame_at_or_before_boundary = true;
        prefix_len = prefix_len.saturating_add(
            (FRAME_HEADER_LEN as u64).saturating_add(parsed_header.payload_len as u64),
        );
        if parsed_header.frame_seq == published_frame {
            return Ok(Some(prefix_len));
        }
    }
}

pub(super) fn write_published_highwater_marker(
    dir: &Path,
    path: &Path,
    tmp_path: &Path,
    highwater: WalHighWatermark,
    sync: bool,
) -> Result<()> {
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
