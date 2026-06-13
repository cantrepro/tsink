use super::segments::{
    open_segment_for_append, segment_path, sync_dir_path, write_published_highwater_marker,
};
use super::*;

#[derive(Debug, Clone, Copy)]
struct LogicalWriteProgress {
    frame_start_len: u64,
    appended_bytes: u64,
    active_segment: u64,
    next_seq: u64,
    last_frame_seq: Option<u64>,
}

impl LogicalWriteProgress {
    fn note_frame_appended(&mut self, appended_bytes: u64) {
        self.last_frame_seq = Some(self.next_seq);
        self.next_seq = self.next_seq.saturating_add(1);
        self.appended_bytes = self.appended_bytes.saturating_add(appended_bytes);
    }

    fn has_frames(&self) -> bool {
        self.last_frame_seq.is_some()
    }

    fn pending_highwater(&self) -> WalHighWatermark {
        self.last_frame_seq
            .map(|frame| WalHighWatermark {
                segment: self.active_segment,
                frame,
            })
            .unwrap_or_default()
    }

    fn active_segment_size_after_append(&self) -> u64 {
        self.frame_start_len.saturating_add(self.appended_bytes)
    }
}

#[derive(Debug, Default)]
struct LogicalWritePublication {
    cached_series_definition_frames: Vec<CachedSeriesDefinitionFrame>,
    persisted: bool,
    synced: bool,
    closed: bool,
    failed: bool,
}

impl LogicalWritePublication {
    fn ensure_writeable(&self, operation: &str) -> Result<()> {
        if self.failed {
            return Err(TsinkError::Wal {
                operation: operation.to_string(),
                details: "logical write already failed".to_string(),
            });
        }
        if self.closed {
            return Err(TsinkError::Wal {
                operation: operation.to_string(),
                details: "logical write already finalized".to_string(),
            });
        }

        Ok(())
    }
}

pub(in crate::engine) struct LogicalWalWrite<'a> {
    wal: &'a FramedWal,
    writer: MutexGuard<'a, BufWriter<File>>,
    progress: LogicalWriteProgress,
    publication: LogicalWritePublication,
}

impl LogicalWalWrite<'_> {
    pub(in crate::engine) fn append_series_definition_payload(
        &mut self,
        payload: &[u8],
    ) -> Result<()> {
        let definition = decode_series_definition(payload)?;
        self.append_frame(FRAME_TYPE_SERIES_DEF, payload)?;
        self.publication
            .cached_series_definition_frames
            .push(CachedSeriesDefinitionFrame::SeriesDefinition(definition));
        Ok(())
    }

    pub(in crate::engine) fn append_samples_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let series_ids = decode_samples_series_ids(payload)?;
        self.append_frame(FRAME_TYPE_SAMPLES, payload)?;
        self.publication
            .cached_series_definition_frames
            .push(CachedSeriesDefinitionFrame::Samples(series_ids));
        Ok(())
    }

    pub(in crate::engine) fn flush_pending(&mut self) -> Result<()> {
        self.publication.ensure_writeable("flush logical write")?;

        if !self.progress.has_frames() {
            return Ok(());
        }

        if let Err(err) = self.writer.flush() {
            self.publication.failed = true;
            return Err(self.rollback_after_io_failure("flush logical write", err));
        }

        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::engine) fn commit(mut self) -> Result<WalHighWatermark> {
        self.publication.ensure_writeable("commit logical write")?;

        let committed_highwater = self.persist_pending()?;
        if committed_highwater == WalHighWatermark::default() {
            self.publication.closed = true;
            return Ok(committed_highwater);
        }

        let rotated = match self.wal.rotate_if_needed(
            &mut self.writer,
            self.progress.active_segment_size_after_append(),
        ) {
            Ok(rotated) => rotated,
            Err(err) => {
                return Err(self.rollback_after_failure("commit logical write rollback", err))
            }
        };

        self.wal.publish_appended_write(
            self.progress.frame_start_len,
            self.progress.appended_bytes,
            self.progress.next_seq,
            committed_highwater,
            self.publication.synced || rotated,
            rotated,
        );
        self.wal
            .publish_committed_highwater(committed_highwater, self.publication.synced || rotated)?;
        self.wal
            .apply_cached_series_definition_frames_if_initialized(std::mem::take(
                &mut self.publication.cached_series_definition_frames,
            ));
        self.publication.closed = true;

        Ok(committed_highwater)
    }

    pub(in crate::engine) fn persist_pending(&mut self) -> Result<WalHighWatermark> {
        self.publication.ensure_writeable("persist logical write")?;

        if self.publication.persisted {
            return Ok(self.progress.pending_highwater());
        }

        if !self.progress.has_frames() {
            self.publication.persisted = true;
            return Ok(WalHighWatermark::default());
        }

        self.flush_pending()?;
        self.publication.synced = match self.wal.sync_after_append_if_needed(&mut self.writer) {
            Ok(synced) => synced,
            Err(err) => {
                return Err(self.rollback_after_failure("persist logical write rollback", err));
            }
        };
        self.publication.persisted = true;

        Ok(self.progress.pending_highwater())
    }

    pub(in crate::engine) fn publish_persisted(mut self) -> Result<WalHighWatermark> {
        assert!(
            !self.publication.failed,
            "cannot publish a logical WAL write after it has failed"
        );
        assert!(
            !self.publication.closed,
            "cannot publish a logical WAL write after it has been closed"
        );
        assert!(
            self.publication.persisted,
            "logical WAL write must be persisted before publication"
        );

        let committed_highwater = self.progress.pending_highwater();
        if committed_highwater > WalHighWatermark::default() {
            self.wal.publish_appended_write(
                self.progress.frame_start_len,
                self.progress.appended_bytes,
                self.progress.next_seq,
                committed_highwater,
                self.publication.synced,
                false,
            );
            self.wal
                .publish_committed_highwater(committed_highwater, self.publication.synced)?;
            self.wal
                .apply_cached_series_definition_frames_if_initialized(std::mem::take(
                    &mut self.publication.cached_series_definition_frames,
                ));
        }
        self.publication.closed = true;
        Ok(committed_highwater)
    }

    pub(in crate::engine) fn abort(mut self) -> Result<()> {
        if self.publication.closed {
            return Ok(());
        }

        self.publication.closed = true;
        if !self.progress.has_frames() {
            return Ok(());
        }

        self.wal
            .rollback_partial_append(&mut self.writer, self.progress.frame_start_len)
    }

    fn append_frame(&mut self, frame_type: u8, payload: &[u8]) -> Result<()> {
        self.publication.ensure_writeable("append logical write")?;

        if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "WAL frame payload too large: {} bytes",
                payload.len()
            )));
        }

        let payload_crc32 = checksum32(payload);
        let mut header = [0u8; FRAME_HEADER_LEN];
        header[0..4].copy_from_slice(&FRAME_MAGIC);
        header[4] = frame_type;
        write_u64_at(&mut header, 8, self.progress.next_seq)?;
        write_u32_at(&mut header, 16, payload.len() as u32)?;
        write_u32_at(&mut header, 20, payload_crc32)?;
        let appended_bytes = FramedWal::frame_size_bytes_for_payload_len(payload.len());

        let write_result = self
            .writer
            .write_all(&header)
            .and_then(|_| self.writer.write_all(payload));
        if let Err(err) = write_result {
            self.publication.failed = true;
            return Err(self.rollback_after_io_failure("append logical write", err));
        }

        self.progress.note_frame_appended(appended_bytes);
        Ok(())
    }

    fn rollback_after_io_failure(
        &mut self,
        operation: &str,
        write_err: std::io::Error,
    ) -> TsinkError {
        self.publication.closed = true;
        if let Err(recovery_err) = self
            .wal
            .rollback_partial_append(&mut self.writer, self.progress.frame_start_len)
        {
            return TsinkError::Wal {
                operation: format!("{operation} rollback"),
                details: format!("append failed: {write_err}; rollback failed: {recovery_err}"),
            };
        }

        write_err.into()
    }

    fn rollback_after_failure(&mut self, operation: &str, err: TsinkError) -> TsinkError {
        self.publication.failed = true;
        self.publication.closed = true;
        match self.wal.rollback_after_failure(
            &mut self.writer,
            self.progress.frame_start_len,
            operation,
            err,
        ) {
            Err(rollback_err) => rollback_err,
            Ok(()) => unreachable!("WAL rollback helper must return an error on failure paths"),
        }
    }
}

impl Drop for LogicalWalWrite<'_> {
    fn drop(&mut self) {
        if self.publication.closed || !self.progress.has_frames() {
            return;
        }

        if let Err(err) = self
            .wal
            .rollback_partial_append(&mut self.writer, self.progress.frame_start_len)
        {
            let _ = self.wal.refresh_runtime_accounting();
            warn!(error = %err, "logical WAL write dropped before publication");
        }
    }
}

impl FramedWal {
    pub fn append_series_definition_payload(&self, payload: &[u8]) -> Result<()> {
        let definition = decode_series_definition(payload)?;
        self.append_frame(FRAME_TYPE_SERIES_DEF, payload)?;
        self.apply_cached_series_definition_frame_if_initialized(
            CachedSeriesDefinitionFrame::SeriesDefinition(definition),
        );
        Ok(())
    }

    pub fn append_samples_payload(&self, payload: &[u8]) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }

        let series_ids = decode_samples_series_ids(payload)?;
        self.append_frame(FRAME_TYPE_SAMPLES, payload)?;
        self.apply_cached_series_definition_frame_if_initialized(
            CachedSeriesDefinitionFrame::Samples(series_ids),
        );
        Ok(())
    }

    pub fn append_series_definition(&self, definition: &SeriesDefinitionFrame) -> Result<()> {
        let payload = Self::encode_series_definition_frame_payload(definition)?;
        self.append_series_definition_payload(&payload)
    }

    pub fn append_samples(&self, batches: &[SamplesBatchFrame]) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let payload = Self::encode_samples_frame_payload(batches)?;
        self.append_samples_payload(&payload)
    }

    pub(in crate::engine) fn begin_logical_write(
        &self,
        estimated_bytes: u64,
    ) -> Result<LogicalWalWrite<'_>> {
        let mut writer = self.writer.lock();
        self.rotate_before_append_if_needed(&mut writer, estimated_bytes)?;

        Ok(LogicalWalWrite {
            wal: self,
            writer,
            progress: LogicalWriteProgress {
                frame_start_len: self.active_segment_size_bytes(),
                appended_bytes: 0,
                active_segment: self.active_segment.load(Ordering::SeqCst),
                next_seq: self.next_seq.load(Ordering::SeqCst),
                last_frame_seq: None,
            },
            publication: LogicalWritePublication::default(),
        })
    }

    pub fn current_highwater(&self) -> WalHighWatermark {
        self.current_appended_highwater()
    }

    pub fn current_appended_highwater(&self) -> WalHighWatermark {
        *self.last_appended_highwater.lock()
    }

    pub fn current_published_highwater(&self) -> WalHighWatermark {
        *self.last_published_highwater.lock()
    }

    pub fn current_durable_highwater(&self) -> WalHighWatermark {
        *self.last_durable_highwater.lock()
    }

    pub fn sync_mode(&self) -> WalSyncMode {
        self.sync_mode
    }

    pub(crate) fn mark_durable_through(&self, highwater: WalHighWatermark) {
        let mut durable_highwater = self.last_durable_highwater.lock();
        if *durable_highwater < highwater {
            *durable_highwater = highwater;
        }
    }

    pub(super) fn mark_published_through(&self, highwater: WalHighWatermark) {
        let mut published_highwater = self.last_published_highwater.lock();
        if *published_highwater < highwater {
            *published_highwater = highwater;
        }
    }

    pub(super) fn advance_highwater_floor(&self, min_highwater: WalHighWatermark) {
        let mut appended_highwater = self.last_appended_highwater.lock();
        if *appended_highwater < min_highwater {
            *appended_highwater = min_highwater;
        }
        drop(appended_highwater);
        self.mark_published_through(min_highwater);
        self.mark_durable_through(min_highwater);
    }

    pub(super) fn persist_published_highwater(
        &self,
        highwater: WalHighWatermark,
        sync: bool,
    ) -> Result<()> {
        write_published_highwater_marker(
            &self.dir,
            &self.published_highwater_path,
            &self.published_highwater_tmp_path,
            highwater,
            sync,
        )
    }

    pub(super) fn publish_committed_highwater(
        &self,
        highwater: WalHighWatermark,
        sync: bool,
    ) -> Result<()> {
        if highwater <= self.current_published_highwater() {
            return Ok(());
        }

        self.persist_published_highwater(highwater, sync)?;
        self.mark_published_through(highwater);
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::engine) fn set_append_sync_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self.append_sync_hook.lock() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine) fn clear_append_sync_hook(&self) {
        *self.append_sync_hook.lock() = None;
    }

    fn append_frame(&self, frame_type: u8, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "WAL frame payload too large: {} bytes",
                payload.len()
            )));
        }

        let payload_crc32 = checksum32(payload);

        let mut writer = self.writer.lock();
        let frame_start_len = self.active_segment_size_bytes();
        let frame_seq = self.next_seq.load(Ordering::SeqCst);
        let active_segment = self.active_segment.load(Ordering::SeqCst);
        let appended_bytes = Self::frame_size_bytes_for_payload_len(payload.len());

        let mut header = [0u8; FRAME_HEADER_LEN];
        header[0..4].copy_from_slice(&FRAME_MAGIC);
        header[4] = frame_type;
        write_u64_at(&mut header, 8, frame_seq)?;
        write_u32_at(&mut header, 16, payload.len() as u32)?;
        write_u32_at(&mut header, 20, payload_crc32)?;

        let write_result = writer
            .write_all(&header)
            .and_then(|_| writer.write_all(payload))
            .and_then(|_| writer.flush());
        if let Err(write_err) = write_result {
            if let Err(recovery_err) = self.rollback_partial_append(&mut writer, frame_start_len) {
                return Err(TsinkError::Wal {
                    operation: "append frame rollback".to_string(),
                    details: format!("append failed: {write_err}; rollback failed: {recovery_err}"),
                });
            }

            return Err(write_err.into());
        }

        self.finalize_append(
            &mut writer,
            frame_start_len,
            appended_bytes,
            frame_seq.saturating_add(1),
            WalHighWatermark {
                segment: active_segment,
                frame: frame_seq,
            },
            "append frame rollback",
        )
    }

    fn finalize_append(
        &self,
        writer: &mut BufWriter<File>,
        frame_start_len: u64,
        appended_bytes: u64,
        next_seq: u64,
        highwater: WalHighWatermark,
        rollback_operation: &str,
    ) -> Result<()> {
        let active_segment_size_after_append = frame_start_len.saturating_add(appended_bytes);
        let rotated = match self.rotate_if_needed(writer, active_segment_size_after_append) {
            Ok(rotated) => rotated,
            Err(err) => {
                return self.rollback_after_failure(
                    writer,
                    frame_start_len,
                    rollback_operation,
                    err,
                );
            }
        };

        let synced = if rotated {
            true
        } else {
            match self.sync_after_append_if_needed(writer) {
                Ok(synced) => synced,
                Err(err) => {
                    return self.rollback_after_failure(
                        writer,
                        frame_start_len,
                        rollback_operation,
                        err,
                    );
                }
            }
        };
        self.publish_appended_write(
            frame_start_len,
            appended_bytes,
            next_seq,
            highwater,
            synced,
            rotated,
        );
        self.publish_committed_highwater(highwater, synced)?;
        Ok(())
    }

    fn publish_appended_write(
        &self,
        frame_start_len: u64,
        appended_bytes: u64,
        next_seq: u64,
        highwater: WalHighWatermark,
        synced: bool,
        rotated: bool,
    ) {
        if !rotated {
            let active_segment_size_after_append = frame_start_len.saturating_add(appended_bytes);
            self.next_seq.store(next_seq, Ordering::SeqCst);
            self.active_segment_size_bytes
                .store(active_segment_size_after_append, Ordering::Release);
        }

        self.record_appended_bytes(appended_bytes);
        *self.last_appended_highwater.lock() = highwater;
        if synced {
            self.mark_durable_through(highwater);
        }
    }

    fn sync_after_append_if_needed(&self, writer: &mut BufWriter<File>) -> Result<bool> {
        let should_sync = match self.sync_mode {
            WalSyncMode::PerAppend => true,
            WalSyncMode::Periodic(interval) => self.last_sync.lock().elapsed() >= interval,
        };
        if !should_sync {
            return Ok(false);
        }

        self.invoke_append_sync_hook()?;
        writer.get_mut().sync_data()?;
        *self.last_sync.lock() = Instant::now();
        Ok(true)
    }

    fn rollback_after_failure(
        &self,
        writer: &mut BufWriter<File>,
        frame_start_len: u64,
        operation: &str,
        err: TsinkError,
    ) -> Result<()> {
        if let Err(recovery_err) = self.rollback_partial_append(writer, frame_start_len) {
            return Err(TsinkError::Wal {
                operation: operation.to_string(),
                details: format!("append failed: {err}; rollback failed: {recovery_err}"),
            });
        }

        let _ = self.refresh_runtime_accounting();
        Err(err)
    }

    fn invoke_append_sync_hook(&self) -> Result<()> {
        #[cfg(test)]
        if let Some(hook) = self.append_sync_hook.lock().clone() {
            hook()?;
        }

        Ok(())
    }

    pub(super) fn rollback_partial_append(
        &self,
        writer: &mut BufWriter<File>,
        frame_start_len: u64,
    ) -> Result<()> {
        writer.get_mut().set_len(frame_start_len)?;
        writer.get_mut().sync_data()?;

        let active_path = self.path.lock().clone();
        let replacement = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        let capacity = writer.capacity();
        let old_writer = std::mem::replace(writer, BufWriter::with_capacity(capacity, replacement));
        let _ = old_writer.into_parts();
        self.active_segment_size_bytes
            .store(frame_start_len, Ordering::Release);

        Ok(())
    }

    fn rotate_before_append_if_needed(
        &self,
        writer: &mut BufWriter<File>,
        append_bytes: u64,
    ) -> Result<()> {
        let current_len = self.active_segment_size_bytes();
        if current_len == 0 {
            return Ok(());
        }

        if current_len.saturating_add(append_bytes) <= self.segment_max_bytes {
            return Ok(());
        }

        self.rotate_to_next_segment(writer, false)
    }

    fn rotate_if_needed(
        &self,
        writer: &mut BufWriter<File>,
        active_segment_size_after_append: u64,
    ) -> Result<bool> {
        if active_segment_size_after_append < self.segment_max_bytes {
            return Ok(false);
        }

        self.rotate_to_next_segment(writer, true)?;
        Ok(true)
    }

    fn rotate_to_next_segment(
        &self,
        writer: &mut BufWriter<File>,
        inject_append_sync_hook: bool,
    ) -> Result<()> {
        if inject_append_sync_hook {
            self.invoke_append_sync_hook()?;
        }
        writer.get_mut().sync_data()?;

        let next_segment = self.active_segment.load(Ordering::SeqCst).saturating_add(1);
        let next_path = segment_path(&self.dir, next_segment);
        let (replacement, segment_created, initial_len) = open_segment_for_append(&next_path)?;
        if segment_created {
            self.record_segment_created(initial_len);
        }
        sync_dir_path(&self.dir)?;

        let capacity = writer.capacity();
        let old_writer = std::mem::replace(writer, BufWriter::with_capacity(capacity, replacement));
        let _ = old_writer.into_parts();

        self.active_segment.store(next_segment, Ordering::SeqCst);
        self.active_segment_size_bytes
            .store(initial_len, Ordering::Release);
        self.next_seq.store(1, Ordering::SeqCst);
        *self.path.lock() = next_path;
        *self.last_sync.lock() = Instant::now();

        Ok(())
    }
}
