use super::*;

fn compaction_replacement_dir(data_path: &Path) -> PathBuf {
    data_path.join(COMPACTION_REPLACEMENT_DIR)
}

fn validate_relative_segment_path(path: &str) -> Result<&Path> {
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(TsinkError::DataCorruption(format!(
            "invalid compaction replacement path: {path}"
        )));
    }
    Ok(candidate)
}

fn segment_rel_path(data_path: &Path, segment_root: &Path) -> Result<String> {
    let relative = segment_root.strip_prefix(data_path).map_err(|_| {
        TsinkError::InvalidConfiguration(format!(
            "segment root {} is outside compactor data path {}",
            segment_root.display(),
            data_path.display()
        ))
    })?;

    Ok(relative.to_string_lossy().into_owned())
}

fn replacement_marker_path(data_path: &Path) -> PathBuf {
    let ts_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let nonce = COMPACTION_REPLACEMENT_COUNTER.fetch_add(1, Ordering::Relaxed);
    compaction_replacement_dir(data_path).join(format!("replace-{ts_nanos:016x}-{nonce:016x}.json"))
}

pub(super) fn write_compaction_replacement_marker(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
) -> Result<PathBuf> {
    if source_segments.is_empty() || output_segments.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "compaction replacement marker requires source and output segments".to_string(),
        ));
    }

    let source_segments = source_segments
        .iter()
        .map(|path| segment_rel_path(data_path, path))
        .collect::<Result<Vec<_>>>()?;
    let output_segments = output_segments
        .iter()
        .map(|path| segment_rel_path(data_path, path))
        .collect::<Result<Vec<_>>>()?;
    let marker = CompactionReplacementMarker {
        version: COMPACTION_REPLACEMENT_VERSION,
        source_segments,
        output_segments,
    };

    let marker_dir = compaction_replacement_dir(data_path);
    fs::create_dir_all(&marker_dir)?;
    let marker_path = replacement_marker_path(data_path);
    let payload = serde_json::to_vec(&marker)?;
    write_file_atomically_and_sync_parent(&marker_path, &payload)?;
    Ok(marker_path)
}

fn parse_compaction_replacement_marker(path: &Path) -> Result<CompactionReplacementMarker> {
    let bytes = fs::read(path)?;
    let marker: CompactionReplacementMarker = serde_json::from_slice(&bytes)?;
    if marker.version != COMPACTION_REPLACEMENT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported compaction replacement marker version {}",
            marker.version
        )));
    }
    if marker.source_segments.is_empty() || marker.output_segments.is_empty() {
        return Err(TsinkError::DataCorruption(
            "compaction replacement marker has empty segment sets".to_string(),
        ));
    }
    Ok(marker)
}

fn apply_compaction_replacement_marker(data_path: &Path, marker_path: &Path) -> Result<()> {
    let marker = parse_compaction_replacement_marker(marker_path)?;

    let output_segments = marker
        .output_segments
        .iter()
        .map(|path| {
            let relative = validate_relative_segment_path(path)?;
            Ok(data_path.join(relative))
        })
        .collect::<Result<Vec<_>>>()?;
    let source_segments = marker
        .source_segments
        .iter()
        .map(|path| {
            let relative = validate_relative_segment_path(path)?;
            Ok(data_path.join(relative))
        })
        .collect::<Result<Vec<_>>>()?;

    for output in &output_segments {
        if !output.join("manifest.bin").exists() {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement output segment missing manifest: {}",
                output.display()
            )));
        }
    }

    for source in &source_segments {
        remove_dir_if_exists(source).map_err(|err| TsinkError::IoWithPath {
            path: source.clone(),
            source: err,
        })?;
    }

    match fs::remove_file(marker_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: marker_path.to_path_buf(),
                source: err,
            });
        }
    }

    Ok(())
}

fn rollback_output_segments(output_segments: &[PathBuf]) -> Result<()> {
    for segment in output_segments.iter().rev() {
        remove_dir_if_exists(segment).map_err(|err| TsinkError::IoWithPath {
            path: segment.clone(),
            source: err,
        })?;
    }
    Ok(())
}

pub(in crate::engine) fn finalize_pending_compaction_replacements(data_path: &Path) -> Result<()> {
    let marker_dir = compaction_replacement_dir(data_path);
    let read_dir = match fs::read_dir(&marker_dir) {
        Ok(read_dir) => read_dir,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: marker_dir,
                source: err,
            });
        }
    };

    let mut marker_paths = Vec::new();
    for entry in read_dir {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(TsinkError::IoWithPath {
                    path: marker_dir.clone(),
                    source: err,
                });
            }
        };
        if !entry.file_type()?.is_file() {
            continue;
        }
        marker_paths.push(entry.path());
    }
    marker_paths.sort();

    for marker_path in marker_paths {
        apply_compaction_replacement_marker(data_path, &marker_path)?;
    }

    Ok(())
}

impl Compactor {
    pub(in crate::engine) fn stage_segment_rewrite_with_retention(
        &self,
        segment: &LoadedSegment,
        retention_cutoff: i64,
    ) -> Result<CompactionOutcome> {
        self.rewrite_segments(
            segment.manifest.level,
            &[segment],
            &TombstoneMap::new(),
            Some(retention_cutoff),
            false,
        )
    }

    pub(super) fn compact_segments(
        &self,
        target_level: u8,
        segments: &[&LoadedSegment],
        tombstones: &TombstoneMap,
    ) -> Result<CompactionOutcome> {
        self.rewrite_segments(target_level, segments, tombstones, None, true)
    }

    fn rewrite_segments(
        &self,
        target_level: u8,
        segments: &[&LoadedSegment],
        tombstones: &TombstoneMap,
        retention_cutoff: Option<i64>,
        apply_replacement: bool,
    ) -> Result<CompactionOutcome> {
        let source_wal_highwater = segments
            .iter()
            .map(|segment| segment.manifest.wal_highwater)
            .max()
            .unwrap_or_default();
        let source_roots = segments
            .iter()
            .map(|segment| segment.root.clone())
            .collect::<Vec<_>>();
        let source_segments = segments.len();
        let (series, chunks_by_series) = collect_series_and_chunk_refs(segments)?;
        let source_chunks = chunks_by_series
            .values()
            .map(std::vec::Vec::len)
            .sum::<usize>();
        let source_points = chunks_by_series
            .values()
            .flat_map(|chunks| chunks.iter())
            .map(|chunk| chunk.header.point_count as usize)
            .sum::<usize>();

        if chunks_by_series.is_empty() {
            return Ok(CompactionOutcome {
                stats: CompactionRunStats {
                    compacted: false,
                    source_level: None,
                    target_level: Some(target_level),
                    source_segments,
                    output_segments: 0,
                    source_chunks,
                    output_chunks: 0,
                    source_points,
                    output_points: 0,
                },
                source_roots,
                output_roots: Vec::new(),
            });
        }

        let registry = SeriesRegistry::new();
        for series_def in &series {
            registry.register_series_with_id(
                series_def.series_id,
                &series_def.metric,
                &series_def.labels,
            )?;
        }

        let output_segment_point_budget = self
            .point_cap
            .saturating_mul(DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER)
            .max(self.point_cap);
        let mut pending_chunks = HashMap::<SeriesId, Vec<Chunk>>::new();
        let mut pending_points = 0usize;
        let mut emitted_segments = 0usize;
        let mut emitted_chunks = 0usize;
        let mut emitted_points = 0usize;
        let mut output_roots = Vec::<PathBuf>::new();
        let write_outputs_result = (|| -> Result<()> {
            for series_def in &series {
                let series_id = series_def.series_id;
                let Some(chunks) = chunks_by_series.get(&series_id) else {
                    continue;
                };
                let tombstone_ranges = tombstones.get(&series_id).map(|ranges| ranges.as_slice());

                stream_merge_series_chunks(
                    series_id,
                    chunks,
                    self.point_cap,
                    tombstone_ranges,
                    retention_cutoff,
                    |chunk| {
                        emitted_chunks = emitted_chunks.saturating_add(1);
                        emitted_points =
                            emitted_points.saturating_add(chunk.header.point_count as usize);
                        pending_points =
                            pending_points.saturating_add(chunk.header.point_count as usize);
                        pending_chunks.entry(series_id).or_default().push(chunk);
                        if pending_points >= output_segment_point_budget {
                            let output_root = self.flush_compacted_segment(
                                target_level,
                                &registry,
                                &pending_chunks,
                                source_wal_highwater,
                            )?;
                            output_roots.push(output_root);
                            pending_chunks.clear();
                            pending_points = 0;
                            emitted_segments = emitted_segments.saturating_add(1);
                        }
                        Ok(())
                    },
                )?;
            }

            if !pending_chunks.is_empty() {
                let output_root = self.flush_compacted_segment(
                    target_level,
                    &registry,
                    &pending_chunks,
                    source_wal_highwater,
                )?;
                output_roots.push(output_root);
                emitted_segments = emitted_segments.saturating_add(1);
            }

            Ok(())
        })();
        if let Err(err) = write_outputs_result {
            if let Err(rollback_err) = rollback_output_segments(&output_roots) {
                return Err(TsinkError::Other(format!(
                    "compaction output write failed and rollback failed: write={err}, rollback={rollback_err}"
                )));
            }
            return Err(err);
        }

        if emitted_segments == 0 {
            return Ok(CompactionOutcome {
                stats: CompactionRunStats {
                    compacted: false,
                    source_level: None,
                    target_level: Some(target_level),
                    source_segments,
                    output_segments: emitted_segments,
                    source_chunks,
                    output_chunks: emitted_chunks,
                    source_points,
                    output_points: emitted_points,
                },
                source_roots,
                output_roots,
            });
        }

        if apply_replacement {
            let replacement_marker_path =
                write_compaction_replacement_marker(&self.data_path, &source_roots, &output_roots)?;
            apply_compaction_replacement_marker(&self.data_path, &replacement_marker_path)?;
        }

        Ok(CompactionOutcome {
            stats: CompactionRunStats {
                compacted: true,
                source_level: None,
                target_level: Some(target_level),
                source_segments,
                output_segments: emitted_segments,
                source_chunks,
                output_chunks: emitted_chunks,
                source_points,
                output_points: emitted_points,
            },
            source_roots,
            output_roots,
        })
    }

    fn flush_compacted_segment(
        &self,
        target_level: u8,
        registry: &SeriesRegistry,
        chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
        wal_highwater: WalHighWatermark,
    ) -> Result<PathBuf> {
        if chunks_by_series.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot flush empty compacted segment".to_string(),
            ));
        }

        let next_segment_id = match &self.next_segment_id {
            Some(next_segment_id) => next_segment_id.fetch_add(1, Ordering::SeqCst),
            None => load_segments_runtime_strict(&self.data_path)?.next_segment_id,
        };
        let writer = SegmentWriter::new(&self.data_path, target_level, next_segment_id)?;
        writer.write_segment_with_wal_highwater(registry, chunks_by_series, wal_highwater)?;
        Ok(writer.layout().root.clone())
    }
}

fn collect_series_and_chunk_refs<'a>(
    segments: &[&'a LoadedSegment],
) -> Result<MergeSegmentsOutput<'a>> {
    let mut series_by_id = BTreeMap::<SeriesId, PersistedSeries>::new();
    let mut chunks_by_series = SeriesChunkRefs::new();

    for segment in segments {
        for series in &segment.series {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels => {}
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts during compaction",
                        series.series_id
                    )));
                }
                None => {
                    series_by_id.insert(series.series_id, series.clone());
                }
            }
        }

        for (series_id, chunks) in &segment.chunks_by_series {
            let entry = chunks_by_series.entry(*series_id).or_default();
            entry.extend(chunks.iter());
        }
    }

    Ok((series_by_id.into_values().collect(), chunks_by_series))
}

fn stream_merge_series_chunks<F>(
    series_id: SeriesId,
    chunks: &[&Chunk],
    point_cap: usize,
    tombstone_ranges: Option<&[TombstoneRange]>,
    retention_cutoff: Option<i64>,
    mut emit_chunk: F,
) -> Result<()>
where
    F: FnMut(Chunk) -> Result<()>,
{
    let lane = infer_lane_for_refs(chunks)?;
    let point_cap = point_cap.max(1);

    let mut cursors = Vec::with_capacity(chunks.len());
    let mut heap = BinaryHeap::<MergeCursorKey>::new();

    for (chunk_order, chunk) in chunks.iter().enumerate() {
        let Some(cursor) = ChunkPointCursor::from_chunk(chunk_order, chunk)? else {
            continue;
        };

        let cursor_idx = cursors.len();
        let first_ts = cursor.current().map(|point| point.ts).unwrap_or_default();
        cursors.push(cursor);
        heap.push(MergeCursorKey {
            ts: first_ts,
            chunk_order,
            cursor_idx,
        });
    }

    if heap.is_empty() {
        return Ok(());
    }

    let mut chunk_points = Vec::with_capacity(point_cap);
    let mut last_emitted_point: Option<ChunkPoint> = None;

    while let Some(key) = heap.pop() {
        let Some(cursor) = cursors.get_mut(key.cursor_idx) else {
            return Err(TsinkError::DataCorruption(
                "compaction cursor index out of bounds".to_string(),
            ));
        };

        let Some(point) = cursor.current().cloned() else {
            return Err(TsinkError::DataCorruption(
                "compaction cursor missing point".to_string(),
            ));
        };

        cursor.advance();
        if let Some(next_point) = cursor.current() {
            heap.push(MergeCursorKey {
                ts: next_point.ts,
                chunk_order: cursor.chunk_order,
                cursor_idx: key.cursor_idx,
            });
        }

        if last_emitted_point
            .as_ref()
            .is_some_and(|previous| previous.ts == point.ts && previous.value == point.value)
        {
            continue;
        }
        if tombstone_ranges.is_some_and(|ranges| timestamp_is_tombstoned(point.ts, ranges)) {
            continue;
        }
        if retention_cutoff.is_some_and(|cutoff| point.ts < cutoff) {
            continue;
        }

        last_emitted_point = Some(point.clone());
        chunk_points.push(point);
        if chunk_points.len() >= point_cap {
            let points = std::mem::replace(&mut chunk_points, Vec::with_capacity(point_cap));
            emit_chunk(encode_compacted_chunk(series_id, lane, points)?)?;
        }
    }

    if !chunk_points.is_empty() {
        emit_chunk(encode_compacted_chunk(series_id, lane, chunk_points)?)?;
    }

    Ok(())
}

fn infer_lane_for_refs(chunks: &[&Chunk]) -> Result<ValueLane> {
    let Some(first) = chunks.first() else {
        return Ok(ValueLane::Numeric);
    };

    let expected = first.header.lane;
    for chunk in chunks {
        if chunk.header.lane != expected {
            return Err(TsinkError::DataCorruption(
                "series mixes numeric and blob lanes across chunks".to_string(),
            ));
        }
    }

    Ok(expected)
}

fn encode_compacted_chunk(
    series_id: SeriesId,
    lane: ValueLane,
    points: Vec<ChunkPoint>,
) -> Result<Chunk> {
    if points.is_empty() {
        return Err(TsinkError::DataCorruption(
            "attempted to encode empty compacted chunk".to_string(),
        ));
    }

    let encoded = Encoder::encode_chunk_points(&points, lane)?;
    let point_count = u16::try_from(points.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("compacted chunk point_count exceeds u16".to_string())
    })?;

    let min_ts = points.first().map(|point| point.ts).unwrap_or(0);
    let max_ts = points.last().map(|point| point.ts).unwrap_or(min_ts);

    Ok(Chunk {
        header: ChunkHeader {
            series_id,
            lane,
            value_family: Some(Encoder::infer_series_value_family(&points, lane)?),
            point_count,
            min_ts,
            max_ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    })
}

pub(super) fn decode_chunk_points_for_compaction(chunk: &Chunk) -> Result<Vec<ChunkPoint>> {
    chunk.decode_points()
}

#[derive(Debug)]
struct ChunkPointCursor {
    chunk_order: usize,
    points: Vec<ChunkPoint>,
    point_idx: usize,
}

impl ChunkPointCursor {
    fn from_chunk(chunk_order: usize, chunk: &Chunk) -> Result<Option<Self>> {
        let points = decode_chunk_points_for_compaction(chunk)?;
        if points.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self {
            chunk_order,
            points,
            point_idx: 0,
        }))
    }

    fn current(&self) -> Option<&ChunkPoint> {
        self.points.get(self.point_idx)
    }

    fn advance(&mut self) {
        self.point_idx = self.point_idx.saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MergeCursorKey {
    ts: i64,
    chunk_order: usize,
    cursor_idx: usize,
}

impl PartialOrd for MergeCursorKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeCursorKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .ts
            .cmp(&self.ts)
            .then_with(|| other.chunk_order.cmp(&self.chunk_order))
            .then_with(|| other.cursor_idx.cmp(&self.cursor_idx))
    }
}
