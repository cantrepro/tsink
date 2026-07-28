use super::codec::{decode_samples_payload, decode_series_definition};
use super::segments::{
    collect_wal_segment_files, open_existing_wal_segment_for_read, WalSegmentFile,
};
use super::*;

#[derive(Clone, Copy)]
pub(super) enum SalvageRecovery {
    SkipFrame,
    SkipSegment,
}

#[derive(Clone, Copy)]
pub(super) enum WalReplayContext {
    General,
    SeriesDefinitionCacheRebuild,
}

impl WalReplayContext {
    fn log_label(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::SeriesDefinitionCacheRebuild => "series_definition_cache_rebuild",
        }
    }
}

impl SalvageRecovery {
    fn log_action(self) -> &'static str {
        match self {
            Self::SkipFrame => "skip_frame",
            Self::SkipSegment => "skip_segment",
        }
    }

    fn apply(self, stream: &mut WalReplayStream) {
        if matches!(self, Self::SkipSegment) {
            stream.current_reader = None;
        }
    }
}

pub struct WalReplayStream {
    segments: Vec<WalSegmentFile>,
    replay_highwater: WalHighWatermark,
    replay_mode: WalReplayMode,
    context: WalReplayContext,
    next_segment_idx: usize,
    current_segment_id: u64,
    current_reader: Option<BufReader<File>>,
    last_frame_highwater: WalHighWatermark,
}

pub struct CommittedWalReplayStream {
    raw: WalReplayStream,
    pending_series_definitions: Vec<SeriesDefinitionFrame>,
    pending_series_definition_bytes: usize,
    published_highwater: WalHighWatermark,
    finished: bool,
}

pub(in crate::engine) struct CommittedWalFrameReplayStream {
    raw: WalReplayStream,
    published_highwater: WalHighWatermark,
    finished: bool,
}

pub(in crate::engine) struct AdmittedReplayFrame<R> {
    pub(in crate::engine) frame: ReplayFrame,
    pub(in crate::engine) reservation: R,
    pub(in crate::engine) highwater: WalHighWatermark,
}

fn modeled_replay_frame_scratch_bytes(frame_type: u8, payload_len: usize) -> Result<usize> {
    let copied_payloads = payload_len
        .checked_mul(2)
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    let decoded_structures = match frame_type {
        FRAME_TYPE_SERIES_DEF => std::mem::size_of::<SeriesDefinitionFrame>()
            .checked_add(payload_len)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?,
        FRAME_TYPE_SAMPLES => (u16::MAX as usize)
            .checked_mul(std::mem::size_of::<SamplesBatchFrame>())
            .ok_or(TsinkError::WriteBatchSizeOverflow)?,
        _ => 0,
    };
    copied_payloads
        .checked_add(decoded_structures)
        .ok_or(TsinkError::WriteBatchSizeOverflow)
}

impl WalReplayStream {
    #[cfg_attr(not(test), allow(dead_code))]
    fn new(
        segments: Vec<WalSegmentFile>,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Self {
        Self::new_with_context(
            segments,
            replay_highwater,
            replay_mode,
            WalReplayContext::General,
        )
    }

    fn new_with_context(
        segments: Vec<WalSegmentFile>,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
        context: WalReplayContext,
    ) -> Self {
        Self {
            segments,
            replay_highwater,
            replay_mode,
            context,
            next_segment_idx: 0,
            current_segment_id: 0,
            current_reader: None,
            last_frame_highwater: WalHighWatermark::default(),
        }
    }

    fn recover_or_fail_corruption(
        &mut self,
        segment_id: u64,
        frame_seq: Option<u64>,
        reason: &str,
        recovery: SalvageRecovery,
    ) -> Result<()> {
        if let Some(frame_seq) = frame_seq {
            warn!(
                context = self.context.log_label(),
                segment = segment_id,
                frame = frame_seq,
                reason,
                recovery = recovery.log_action(),
                "WAL replay corruption"
            );
        } else {
            warn!(
                context = self.context.log_label(),
                segment = segment_id,
                reason,
                recovery = recovery.log_action(),
                "WAL replay corruption"
            );
        }

        match self.replay_mode {
            WalReplayMode::Salvage => {
                recovery.apply(self);
                Ok(())
            }
            WalReplayMode::Strict => {
                Err(wal_replay_corruption_error(segment_id, frame_seq, reason))
            }
        }
    }

    fn recover_or_fail_decode_error(
        &mut self,
        segment_id: u64,
        frame_seq: u64,
        decode_err: TsinkError,
        recovery: SalvageRecovery,
    ) -> Result<()> {
        warn!(
            context = self.context.log_label(),
            segment = segment_id,
            frame = frame_seq,
            error = %decode_err,
            recovery = recovery.log_action(),
            "WAL replay decode failure"
        );
        match self.replay_mode {
            WalReplayMode::Salvage => {
                recovery.apply(self);
                Ok(())
            }
            WalReplayMode::Strict => Err(wal_replay_corruption_error(
                segment_id,
                Some(frame_seq),
                &decode_err.to_string(),
            )),
        }
    }

    pub fn next_frame(&mut self) -> Result<Option<ReplayFrame>> {
        Ok(self
            .next_frame_with_admission(|_| Ok(()))?
            .map(|admitted| admitted.frame))
    }

    pub(in crate::engine) fn next_frame_with_admission<R>(
        &mut self,
        mut reserve: impl FnMut(usize) -> Result<R>,
    ) -> Result<Option<AdmittedReplayFrame<R>>> {
        loop {
            if self.current_reader.is_none() && !self.open_next_segment()? {
                return Ok(None);
            }

            let segment_id = self.current_segment_id;
            let reader = self.current_reader.as_mut().expect("reader set above");

            let header = match read_header(reader)? {
                HeaderRead::Eof => {
                    self.current_reader = None;
                    continue;
                }
                HeaderRead::Truncated => {
                    self.recover_or_fail_corruption(
                        segment_id,
                        None,
                        "truncated frame header",
                        SalvageRecovery::SkipSegment,
                    )?;
                    continue;
                }
                HeaderRead::FrameHeader(header) => header,
            };

            let Some(parsed_header) = parse_frame_header(&header)? else {
                self.recover_or_fail_corruption(
                    segment_id,
                    None,
                    "frame with magic mismatch",
                    SalvageRecovery::SkipSegment,
                )?;
                continue;
            };
            let frame_type = parsed_header.frame_type;
            let frame_seq = parsed_header.frame_seq;
            let payload_len = parsed_header.payload_len;
            let expected_crc32 = parsed_header.expected_crc32;

            if payload_len > MAX_FRAME_PAYLOAD_BYTES {
                self.recover_or_fail_corruption(
                    segment_id,
                    Some(frame_seq),
                    "oversized frame payload",
                    SalvageRecovery::SkipSegment,
                )?;
                continue;
            }

            let reservation =
                reserve(modeled_replay_frame_scratch_bytes(frame_type, payload_len)?)?;

            let mut payload = vec![0u8; payload_len];
            if let Err(e) = reader.read_exact(&mut payload) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.recover_or_fail_corruption(
                        segment_id,
                        Some(frame_seq),
                        "truncated frame payload",
                        SalvageRecovery::SkipSegment,
                    )?;
                    continue;
                }
                return Err(e.into());
            }

            if checksum32(&payload) != expected_crc32 {
                self.recover_or_fail_corruption(
                    segment_id,
                    Some(frame_seq),
                    "frame with checksum mismatch",
                    SalvageRecovery::SkipFrame,
                )?;
                continue;
            }

            let frame_pos = WalHighWatermark {
                segment: segment_id,
                frame: frame_seq,
            };
            if frame_pos <= self.replay_highwater {
                continue;
            }

            let frame = match frame_type {
                FRAME_TYPE_SERIES_DEF => match decode_series_definition(&payload) {
                    Ok(frame) => ReplayFrame::SeriesDefinition(frame),
                    Err(err) => {
                        self.recover_or_fail_decode_error(
                            segment_id,
                            frame_seq,
                            err,
                            SalvageRecovery::SkipFrame,
                        )?;
                        continue;
                    }
                },
                FRAME_TYPE_SAMPLES => match decode_samples_payload(&payload) {
                    Ok(frame) => ReplayFrame::Samples(frame),
                    Err(err) => {
                        self.recover_or_fail_decode_error(
                            segment_id,
                            frame_seq,
                            err,
                            SalvageRecovery::SkipFrame,
                        )?;
                        continue;
                    }
                },
                _ => {
                    self.recover_or_fail_corruption(
                        segment_id,
                        Some(frame_seq),
                        "unknown frame type",
                        SalvageRecovery::SkipFrame,
                    )?;
                    continue;
                }
            };

            self.last_frame_highwater = frame_pos;
            return Ok(Some(AdmittedReplayFrame {
                frame,
                reservation,
                highwater: frame_pos,
            }));
        }
    }

    fn last_frame_highwater(&self) -> WalHighWatermark {
        self.last_frame_highwater
    }

    fn open_next_segment(&mut self) -> Result<bool> {
        let Some(segment) = self.segments.get(self.next_segment_idx) else {
            return Ok(false);
        };
        self.next_segment_idx += 1;

        let file = open_existing_wal_segment_for_read(&segment.path)?;

        self.current_segment_id = segment.id;
        self.current_reader = Some(BufReader::new(file));
        Ok(true)
    }
}

impl CommittedWalReplayStream {
    fn new(raw: WalReplayStream, published_highwater: WalHighWatermark) -> Self {
        Self {
            raw,
            pending_series_definitions: Vec::new(),
            pending_series_definition_bytes: 0,
            published_highwater,
            finished: false,
        }
    }

    pub fn next_write(&mut self) -> Result<Option<CommittedWalWriteFrame>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let next_frame = self.raw.next_frame()?;
            if self.raw.last_frame_highwater() > self.published_highwater {
                self.pending_series_definitions.clear();
                self.finished = true;
                return Ok(None);
            }

            match next_frame {
                Some(ReplayFrame::SeriesDefinition(definition)) => {
                    let definition_bytes = std::mem::size_of::<SeriesDefinitionFrame>()
                        .checked_add(definition.metric.len())
                        .and_then(|bytes| {
                            definition
                                .labels
                                .len()
                                .checked_mul(std::mem::size_of::<Label>())
                                .and_then(|labels| bytes.checked_add(labels))
                        })
                        .and_then(|bytes| {
                            definition.labels.iter().try_fold(bytes, |bytes, label| {
                                bytes
                                    .checked_add(label.name.len())
                                    .and_then(|bytes| bytes.checked_add(label.value.len()))
                            })
                        })
                        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
                    let pending_bytes = self
                        .pending_series_definition_bytes
                        .checked_add(definition_bytes)
                        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
                    if pending_bytes > MAX_FRAME_PAYLOAD_BYTES {
                        return Err(TsinkError::DataCorruption(format!(
                            "committed WAL write has more than {MAX_FRAME_PAYLOAD_BYTES} bytes of pending series definitions"
                        )));
                    }
                    self.pending_series_definition_bytes = pending_bytes;
                    self.pending_series_definitions.push(definition);
                }
                Some(ReplayFrame::Samples(sample_batches)) => {
                    let committed_series_ids = sample_batches
                        .iter()
                        .map(|batch| batch.series_id)
                        .collect::<BTreeSet<_>>();
                    let series_definitions = std::mem::take(&mut self.pending_series_definitions)
                        .into_iter()
                        .filter(|definition| committed_series_ids.contains(&definition.series_id))
                        .collect();
                    self.pending_series_definition_bytes = 0;
                    return Ok(Some(CommittedWalWriteFrame {
                        series_definitions,
                        sample_batches,
                        highwater: self.raw.last_frame_highwater(),
                    }));
                }
                None => {
                    self.pending_series_definitions.clear();
                    self.finished = true;
                    return Ok(None);
                }
            }
        }
    }
}

impl CommittedWalFrameReplayStream {
    fn new(raw: WalReplayStream, published_highwater: WalHighWatermark) -> Self {
        Self {
            raw,
            published_highwater,
            finished: false,
        }
    }

    pub(in crate::engine) fn next_frame_with_admission<R>(
        &mut self,
        mut reserve: impl FnMut(usize) -> Result<R>,
    ) -> Result<Option<AdmittedReplayFrame<R>>> {
        if self.finished {
            return Ok(None);
        }
        let Some(frame) = self.raw.next_frame_with_admission(&mut reserve)? else {
            self.finished = true;
            return Ok(None);
        };
        if frame.highwater > self.published_highwater {
            self.finished = true;
            return Ok(None);
        }
        Ok(Some(frame))
    }
}

pub(super) fn wal_replay_corruption_error(
    segment_id: u64,
    frame_seq: Option<u64>,
    reason: &str,
) -> TsinkError {
    match frame_seq {
        Some(frame_seq) => TsinkError::DataCorruption(format!(
            "WAL replay corruption at segment {segment_id}, frame {frame_seq}: {reason}"
        )),
        None => TsinkError::DataCorruption(format!(
            "WAL replay corruption at segment {segment_id}: {reason}"
        )),
    }
}

pub(super) enum HeaderRead {
    Eof,
    Truncated,
    FrameHeader([u8; FRAME_HEADER_LEN]),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ParsedFrameHeader {
    pub(super) frame_type: u8,
    pub(super) frame_seq: u64,
    pub(super) payload_len: usize,
    pub(super) expected_crc32: u32,
}

pub(super) fn parse_frame_header(
    header: &[u8; FRAME_HEADER_LEN],
) -> Result<Option<ParsedFrameHeader>> {
    if header[0..4] != FRAME_MAGIC {
        return Ok(None);
    }

    Ok(Some(ParsedFrameHeader {
        frame_type: header[4],
        frame_seq: read_u64_at(header, 8)?,
        payload_len: read_u32_at(header, 16)? as usize,
        expected_crc32: read_u32_at(header, 20)?,
    }))
}

pub(super) fn read_header(reader: &mut BufReader<File>) -> Result<HeaderRead> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    let mut offset = 0usize;
    while offset < FRAME_HEADER_LEN {
        match reader.read(&mut header[offset..]) {
            Ok(0) if offset == 0 => return Ok(HeaderRead::Eof),
            Ok(0) => return Ok(HeaderRead::Truncated),
            Ok(read) => {
                offset += read;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(HeaderRead::FrameHeader(header))
}

impl FramedWal {
    pub fn replay_frames(&self) -> Result<Vec<ReplayFrame>> {
        self.replay_frames_after_with_mode(WalHighWatermark::default(), WalReplayMode::Strict)
    }

    pub fn replay_frames_with_mode(&self, replay_mode: WalReplayMode) -> Result<Vec<ReplayFrame>> {
        self.replay_frames_after_with_mode(WalHighWatermark::default(), replay_mode)
    }

    pub fn replay_frames_after(
        &self,
        replay_highwater: WalHighWatermark,
    ) -> Result<Vec<ReplayFrame>> {
        self.replay_frames_after_with_mode(replay_highwater, WalReplayMode::Strict)
    }

    pub fn replay_frames_after_with_mode(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Result<Vec<ReplayFrame>> {
        let mut stream = self.replay_stream_after_with_mode(replay_highwater, replay_mode)?;
        let mut out = Vec::new();
        while let Some(frame) = stream.next_frame()? {
            out.push(frame);
        }
        Ok(out)
    }

    pub fn replay_committed_writes(&self) -> Result<Vec<CommittedWalWriteFrame>> {
        self.replay_committed_writes_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Strict,
        )
    }

    pub fn replay_committed_writes_after(
        &self,
        replay_highwater: WalHighWatermark,
    ) -> Result<Vec<CommittedWalWriteFrame>> {
        self.replay_committed_writes_after_with_mode(replay_highwater, WalReplayMode::Strict)
    }

    pub fn replay_committed_writes_after_with_mode(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Result<Vec<CommittedWalWriteFrame>> {
        let mut stream =
            self.replay_committed_write_stream_after_with_mode(replay_highwater, replay_mode)?;
        let mut out = Vec::new();
        while let Some(write) = stream.next_write()? {
            out.push(write);
        }
        Ok(out)
    }

    pub fn replay_committed_series_definitions(&self) -> Result<Vec<SeriesDefinitionFrame>> {
        self.replay_committed_series_definitions_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Strict,
        )
    }

    pub fn replay_committed_series_definitions_after_with_mode(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Result<Vec<SeriesDefinitionFrame>> {
        let writes = self.replay_committed_writes_after_with_mode(replay_highwater, replay_mode)?;
        Ok(writes
            .into_iter()
            .flat_map(|write| write.series_definitions)
            .collect())
    }

    pub fn replay_stream_after(
        &self,
        replay_highwater: WalHighWatermark,
    ) -> Result<WalReplayStream> {
        self.replay_stream_after_with_mode(replay_highwater, WalReplayMode::Strict)
    }

    pub fn replay_stream_after_with_mode(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Result<WalReplayStream> {
        self.replay_stream_after_with_context(
            replay_highwater,
            replay_mode,
            WalReplayContext::General,
        )
    }

    fn replay_stream_after_with_context(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
        context: WalReplayContext,
    ) -> Result<WalReplayStream> {
        let segments = collect_wal_segment_files(&self.dir)?;
        let replay_highwater =
            replay_highwater.max(self.current_reset_highwater_floor().unwrap_or_default());
        Ok(WalReplayStream::new_with_context(
            segments,
            replay_highwater,
            replay_mode,
            context,
        ))
    }

    pub fn replay_committed_write_stream_after(
        &self,
        replay_highwater: WalHighWatermark,
    ) -> Result<CommittedWalReplayStream> {
        self.replay_committed_write_stream_after_with_mode(replay_highwater, WalReplayMode::Strict)
    }

    pub fn replay_committed_write_stream_after_with_mode(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Result<CommittedWalReplayStream> {
        self.replay_committed_write_stream_after_with_context(
            replay_highwater,
            replay_mode,
            WalReplayContext::General,
        )
    }

    pub(super) fn replay_committed_write_stream_after_with_context(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
        context: WalReplayContext,
    ) -> Result<CommittedWalReplayStream> {
        Ok(CommittedWalReplayStream::new(
            self.replay_stream_after_with_context(replay_highwater, replay_mode, context)?,
            self.current_published_highwater(),
        ))
    }

    pub(in crate::engine) fn replay_committed_frame_stream_after_with_mode(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: WalReplayMode,
    ) -> Result<CommittedWalFrameReplayStream> {
        Ok(CommittedWalFrameReplayStream::new(
            self.replay_stream_after_with_context(
                replay_highwater,
                replay_mode,
                WalReplayContext::General,
            )?,
            self.current_published_highwater(),
        ))
    }
}

#[cfg(test)]
pub(super) fn replay_from_path(
    path: &Path,
    replay_highwater: WalHighWatermark,
) -> Result<Vec<ReplayFrame>> {
    replay_from_path_with_mode(path, replay_highwater, WalReplayMode::Strict)
}

#[cfg(test)]
pub(super) fn replay_from_path_with_mode(
    path: &Path,
    replay_highwater: WalHighWatermark,
    replay_mode: WalReplayMode,
) -> Result<Vec<ReplayFrame>> {
    replay_from_segments_with_mode(
        vec![WalSegmentFile {
            id: 0,
            path: path.to_path_buf(),
        }],
        replay_highwater,
        replay_mode,
    )
}

#[cfg(test)]
fn replay_from_segments_with_mode(
    segments: Vec<WalSegmentFile>,
    replay_highwater: WalHighWatermark,
    replay_mode: WalReplayMode,
) -> Result<Vec<ReplayFrame>> {
    let mut stream = WalReplayStream::new(segments, replay_highwater, replay_mode);
    let mut out = Vec::new();
    while let Some(frame) = stream.next_frame()? {
        out.push(frame);
    }

    Ok(out)
}
