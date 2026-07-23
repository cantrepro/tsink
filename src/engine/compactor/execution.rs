use super::*;

fn compaction_replacement_dir(data_path: &Path) -> PathBuf {
    data_path.join(COMPACTION_REPLACEMENT_DIR)
}

fn validate_relative_segment_path(data_path: &Path, path: &str) -> Result<PathBuf> {
    let candidate = Path::new(path);
    let components = candidate.components().collect::<Vec<_>>();
    let [Component::Normal(segments_component), Component::Normal(level_component), Component::Normal(segment_component)] =
        components.as_slice()
    else {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement path must be an exact segment root: {path}"
        )));
    };
    if *segments_component != std::ffi::OsStr::new("segments") {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement path is outside the segment namespace: {path}"
        )));
    }
    let level = level_component.to_str().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "compaction replacement level is not valid UTF-8: {path}"
        ))
    })?;
    let parsed_level = level
        .strip_prefix('L')
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|value| level == format!("L{value}"))
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "compaction replacement path has an invalid level: {path}"
            ))
        })?;
    if parsed_level > 2 {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement path uses unsupported segment level L{parsed_level}: {path}"
        )));
    }
    let segment = segment_component.to_str().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "compaction replacement segment id is not valid UTF-8: {path}"
        ))
    })?;
    let segment_hex = segment.strip_prefix("seg-").ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "compaction replacement path has an invalid segment name: {path}"
        ))
    })?;
    if segment_hex.len() != 16
        || !segment_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement path has an invalid segment id: {path}"
        )));
    }

    let root = fs::canonicalize(data_path).map_err(|source| TsinkError::IoWithPath {
        path: data_path.to_path_buf(),
        source,
    })?;
    let mut resolved = root.clone();
    let path_components = [*segments_component, *level_component, *segment_component];
    for (index, component) in path_components.into_iter().enumerate() {
        resolved.push(component);
        match fs::symlink_metadata(&resolved) {
            Ok(metadata) if crate::engine::fs_utils::is_link_or_reparse_point(&metadata) => {
                return Err(TsinkError::DataCorruption(format!(
                    "compaction replacement path contains a link-like component: {}",
                    resolved.display()
                )))
            }
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(TsinkError::DataCorruption(format!(
                    "compaction replacement path contains a non-directory component: {}",
                    resolved.display()
                )))
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Missing planned outputs and already-retired sources are valid recovery states.
                for suffix in path_components.into_iter().skip(index + 1) {
                    resolved.push(suffix);
                }
                break;
            }
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: resolved,
                    source,
                })
            }
        }
    }
    if resolved == root || !resolved.starts_with(&root) {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement path resolves outside {}: {path}",
            root.display()
        )));
    }
    // Keep the configured lexical prefix for catalog diffs. The component walk above performs
    // the containment and no-link validation against the canonical data root separately.
    Ok(data_path.join(candidate))
}

fn segment_identity_from_root(root: &Path) -> Result<(u8, u64)> {
    let level = root
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix('L'))
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|level| *level <= 2)
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "compaction replacement segment has an invalid level path: {}",
                root.display()
            ))
        })?;
    let segment_id = root
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("seg-"))
        .filter(|value| {
            value.len() == 16
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "compaction replacement segment has an invalid id path: {}",
                root.display()
            ))
        })?;
    Ok((level, segment_id))
}

fn validate_legacy_partial_segment_entries_no_follow(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|source| TsinkError::IoWithPath {
        path: root.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "legacy compaction source is link-like or not a directory: {}",
            root.display()
        )));
    }
    let expected = [
        "chunks.bin",
        "chunk_index.bin",
        "series.bin",
        "postings.bin",
        "manifest.bin",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    for entry in fs::read_dir(root).map_err(|source| TsinkError::IoWithPath {
        path: root.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: root.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "legacy compaction source contains a non-UTF-8 entry: {}",
                entry.path().display()
            ))
        })?;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })?;
        if !expected.contains(name)
            || crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "legacy compaction source contains an unowned entry: {}",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

fn validate_complete_segment(root: &Path) -> Result<LoadedSegment> {
    let (expected_level, expected_segment_id) = segment_identity_from_root(root)?;
    validate_complete_segment_identity(root, expected_level, expected_segment_id)
}

fn validate_complete_segment_identity(
    root: &Path,
    expected_level: u8,
    expected_segment_id: u64,
) -> Result<LoadedSegment> {
    crate::engine::segment::load_complete_segment_no_follow(
        root,
        expected_level,
        expected_segment_id,
        SegmentValidationContext::Compaction,
    )
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

#[cfg(test)]
thread_local! {
    static FORCED_MARKER_CANDIDATE: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) struct ForcedMarkerCandidateGuard;

#[cfg(test)]
impl Drop for ForcedMarkerCandidateGuard {
    fn drop(&mut self) {
        FORCED_MARKER_CANDIDATE.with(|slot| slot.borrow_mut().take());
    }
}

#[cfg(test)]
pub(super) fn force_next_replacement_marker_candidate(
    candidate: PathBuf,
) -> ForcedMarkerCandidateGuard {
    FORCED_MARKER_CANDIDATE.with(|slot| *slot.borrow_mut() = Some(candidate));
    ForcedMarkerCandidateGuard
}

fn replacement_marker_path(data_path: &Path) -> Result<PathBuf> {
    let ts_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    // Engine compaction is serialized and protected by the process lease. Hostile concurrent
    // namespace mutation is outside that contract, but stale crash markers are not: skip every
    // no-follow collision before using the existing atomic publication primitive.
    #[cfg(test)]
    if let Some(candidate) = FORCED_MARKER_CANDIDATE.with(|slot| slot.borrow_mut().take()) {
        match fs::symlink_metadata(&candidate) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
            Ok(_) => {}
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: candidate,
                    source,
                });
            }
        }
    }
    for _ in 0..256 {
        let nonce = COMPACTION_REPLACEMENT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = compaction_replacement_dir(data_path)
            .join(format!("replace-{ts_nanos:016x}-{nonce:016x}.json"));
        match fs::symlink_metadata(&candidate) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
            Ok(_) => continue,
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: candidate,
                    source,
                });
            }
        }
    }
    Err(TsinkError::Other(
        "failed to allocate an unused compaction replacement marker name".to_string(),
    ))
}

fn is_compaction_replacement_marker_name(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix("replace-")
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return false;
    };
    let Some((timestamp, nonce)) = body.split_once('-') else {
        return false;
    };
    timestamp.len() == 16
        && nonce.len() == 16
        && timestamp
            .bytes()
            .chain(nonce.bytes())
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
pub(super) fn write_compaction_replacement_marker(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
) -> Result<PathBuf> {
    let marker_path = replacement_marker_path(data_path)?;
    write_compaction_replacement_marker_at(
        data_path,
        source_segments,
        output_segments,
        CompactionReplacementPhase::Ready,
        &marker_path,
        MarkerWriteMode::CreateNew,
    )
    .map_err(|failure| failure.error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerPublicationState {
    PrePublication,
    Ambiguous,
    Published,
}

#[derive(Debug)]
struct MarkerWriteFailure {
    marker_path: PathBuf,
    state: MarkerPublicationState,
    error: TsinkError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerWriteMode {
    CreateNew,
    ReplaceExisting,
}

fn write_compaction_replacement_marker_at(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
    phase: CompactionReplacementPhase,
    marker_path: &Path,
    mode: MarkerWriteMode,
) -> std::result::Result<PathBuf, MarkerWriteFailure> {
    let payload =
        compaction_replacement_marker_payload(data_path, source_segments, output_segments, phase)
            .map_err(|error| MarkerWriteFailure {
            marker_path: marker_path.to_path_buf(),
            state: MarkerPublicationState::PrePublication,
            error,
        })?;

    let marker_dir = compaction_replacement_dir(data_path);
    crate::engine::fs_utils::create_dir_all_and_sync_parents(&marker_dir).map_err(|error| {
        MarkerWriteFailure {
            marker_path: marker_path.to_path_buf(),
            state: MarkerPublicationState::PrePublication,
            error,
        }
    })?;
    let marker_dir_metadata =
        fs::symlink_metadata(&marker_dir).map_err(|source| MarkerWriteFailure {
            marker_path: marker_path.to_path_buf(),
            state: MarkerPublicationState::PrePublication,
            error: TsinkError::IoWithPath {
                path: marker_dir.clone(),
                source,
            },
        })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&marker_dir_metadata)
        || !marker_dir_metadata.file_type().is_dir()
    {
        return Err(MarkerWriteFailure {
            marker_path: marker_path.to_path_buf(),
            state: MarkerPublicationState::PrePublication,
            error: TsinkError::InvalidConfiguration(format!(
                "compaction replacement marker directory is link-like or not a directory: {}",
                marker_dir.display()
            )),
        });
    }
    match (mode, fs::symlink_metadata(marker_path)) {
        (MarkerWriteMode::CreateNew, Err(err)) if err.kind() == std::io::ErrorKind::NotFound => {}
        (MarkerWriteMode::CreateNew, Ok(_)) => {
            return Err(MarkerWriteFailure {
                marker_path: marker_path.to_path_buf(),
                state: MarkerPublicationState::PrePublication,
                error: TsinkError::InvalidConfiguration(format!(
                    "compaction replacement marker create-new target already exists: {}",
                    marker_path.display()
                )),
            });
        }
        (MarkerWriteMode::CreateNew, Err(source)) => {
            return Err(MarkerWriteFailure {
                marker_path: marker_path.to_path_buf(),
                state: MarkerPublicationState::PrePublication,
                error: TsinkError::IoWithPath {
                    path: marker_path.to_path_buf(),
                    source,
                },
            });
        }
        (MarkerWriteMode::ReplaceExisting, Ok(existing))
            if !crate::engine::fs_utils::is_link_or_reparse_point(&existing)
                && existing.file_type().is_file() => {}
        (MarkerWriteMode::ReplaceExisting, Ok(_)) => {
            return Err(MarkerWriteFailure {
                marker_path: marker_path.to_path_buf(),
                state: MarkerPublicationState::PrePublication,
                error: TsinkError::DataCorruption(format!(
                    "compaction replacement marker target is link-like or not regular: {}",
                    marker_path.display()
                )),
            });
        }
        (MarkerWriteMode::ReplaceExisting, Err(source)) => {
            return Err(MarkerWriteFailure {
                marker_path: marker_path.to_path_buf(),
                state: MarkerPublicationState::PrePublication,
                error: TsinkError::IoWithPath {
                    path: marker_path.to_path_buf(),
                    source,
                },
            });
        }
    }
    let temporary =
        crate::engine::fs_utils::write_tmp_and_sync(marker_path, &payload).map_err(|error| {
            MarkerWriteFailure {
                marker_path: marker_path.to_path_buf(),
                state: MarkerPublicationState::PrePublication,
                error,
            }
        })?;
    if let Err(rename_error) = crate::engine::fs_utils::rename_tmp(&temporary, marker_path) {
        let cleanup_error = crate::engine::fs_utils::remove_file_if_exists(&temporary).err();
        let error = match cleanup_error {
            Some(cleanup_error) => TsinkError::Other(format!(
                "compaction marker rename failed: {rename_error}; temporary cleanup failed: {cleanup_error}"
            )),
            None => rename_error,
        };
        return Err(MarkerWriteFailure {
            marker_path: marker_path.to_path_buf(),
            // Cross-platform rename APIs do not make a failed return a proof that the destination
            // stayed unpublished. Keep the recovery intent and every planned output.
            state: MarkerPublicationState::Ambiguous,
            error,
        });
    }
    if let Err(error) = crate::engine::fs_utils::sync_parent_dir(marker_path) {
        return Err(MarkerWriteFailure {
            marker_path: marker_path.to_path_buf(),
            state: MarkerPublicationState::Published,
            error,
        });
    }
    Ok(marker_path.to_path_buf())
}

fn compaction_replacement_marker_payload(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
    phase: CompactionReplacementPhase,
) -> Result<Vec<u8>> {
    if source_segments.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "compaction replacement marker requires source segments".to_string(),
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
        phase: Some(phase),
        source_segments,
        output_segments,
    };
    Ok(serde_json::to_vec(&marker)?)
}

struct ValidatedCompactionReplacement {
    version: u16,
    phase: CompactionReplacementPhase,
    source_segments: Vec<PathBuf>,
    output_segments: Vec<PathBuf>,
}

fn parse_compaction_replacement_marker(
    data_path: &Path,
    path: &Path,
) -> Result<ValidatedCompactionReplacement> {
    let metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement marker is link-like or not a regular file: {}",
            path.display()
        )));
    }
    let bytes = fs::read(path)?;
    let marker: CompactionReplacementMarker = serde_json::from_slice(&bytes)?;
    let phase = match (marker.version, marker.phase) {
        (LEGACY_COMPACTION_REPLACEMENT_VERSION, None) => CompactionReplacementPhase::Ready,
        (COMPACTION_REPLACEMENT_VERSION, Some(phase)) => phase,
        (LEGACY_COMPACTION_REPLACEMENT_VERSION, Some(_))
        | (COMPACTION_REPLACEMENT_VERSION, None) => {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement marker version {} has an invalid phase",
                marker.version
            )))
        }
        (version, _) => {
            return Err(TsinkError::DataCorruption(format!(
                "unsupported compaction replacement marker version {version}"
            )))
        }
    };
    if marker.source_segments.is_empty() {
        return Err(TsinkError::DataCorruption(
            "compaction replacement marker has no source segments".to_string(),
        ));
    }
    if marker.version == LEGACY_COMPACTION_REPLACEMENT_VERSION && marker.output_segments.is_empty()
    {
        return Err(TsinkError::DataCorruption(
            "legacy compaction replacement marker has no output segments".to_string(),
        ));
    }

    let mut distinct_sources = BTreeSet::new();
    let mut source_segments = Vec::with_capacity(marker.source_segments.len());
    for relative in marker.source_segments {
        if !distinct_sources.insert(relative.clone()) {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement marker repeats source segment {relative}"
            )));
        }
        source_segments.push(validate_relative_segment_path(data_path, &relative)?);
    }
    let mut distinct_outputs = BTreeSet::new();
    let mut output_segments = Vec::with_capacity(marker.output_segments.len());
    for relative in marker.output_segments {
        if !distinct_outputs.insert(relative.clone()) {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement marker repeats output segment {relative}"
            )));
        }
        if distinct_sources.contains(&relative) {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement marker uses the same source and output segment {relative}"
            )));
        }
        output_segments.push(validate_relative_segment_path(data_path, &relative)?);
    }

    Ok(ValidatedCompactionReplacement {
        version: marker.version,
        phase,
        source_segments,
        output_segments,
    })
}

fn marker_matches_plan(
    marker: &ValidatedCompactionReplacement,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
) -> bool {
    marker.source_segments == source_segments && marker.output_segments == output_segments
}

fn recover_preparing_marker_publication_failure(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
    failure: MarkerWriteFailure,
) -> Result<()> {
    let context = TsinkError::Other(format!(
        "compaction preparing marker {:?} publication failed for {}: {}",
        failure.state,
        failure.marker_path.display(),
        failure.error
    ));
    if failure.state == MarkerPublicationState::PrePublication {
        return Err(context);
    }
    crate::engine::fs_utils::sync_parent_dir(&failure.marker_path).map_err(|sync_error| {
        TsinkError::Other(format!(
            "{context}; visible Preparing marker durability retry failed: {sync_error}"
        ))
    })?;
    let marker = parse_compaction_replacement_marker(data_path, &failure.marker_path).map_err(
        |recovery_error| {
            TsinkError::Other(format!(
                "{context}; visible Preparing marker validation failed: {recovery_error}"
            ))
        },
    )?;
    if marker.phase != CompactionReplacementPhase::Preparing
        || !marker_matches_plan(&marker, source_segments, output_segments)
    {
        return Err(TsinkError::Other(format!(
            "{context}; visible marker does not match the intended Preparing plan"
        )));
    }
    Ok(())
}

fn resolve_ready_marker_publication_failure(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
    failure: MarkerWriteFailure,
) -> Result<()> {
    let context = TsinkError::Other(format!(
        "compaction ready marker {:?} publication failed for {}: {}",
        failure.state,
        failure.marker_path.display(),
        failure.error
    ));
    crate::engine::fs_utils::sync_parent_dir(&failure.marker_path).map_err(|sync_error| {
        TsinkError::Other(format!(
            "{context}; visible marker durability retry failed: {sync_error}"
        ))
    })?;
    let marker = parse_compaction_replacement_marker(data_path, &failure.marker_path).map_err(
        |recovery_error| {
            TsinkError::Other(format!(
                "{context}; visible marker validation failed: {recovery_error}"
            ))
        },
    )?;
    if !marker_matches_plan(&marker, source_segments, output_segments) {
        return Err(TsinkError::Other(format!(
            "{context}; visible marker does not match the intended compaction plan"
        )));
    }
    match apply_compaction_replacement_marker(data_path, &failure.marker_path, None) {
        Ok(Some(_)) if marker.phase == CompactionReplacementPhase::Ready => Ok(()),
        Ok(None) if marker.phase == CompactionReplacementPhase::Preparing => Err(context),
        Ok(_) => Err(TsinkError::Other(format!(
            "{context}; visible marker phase changed while resolving publication"
        ))),
        Err(recovery_error) => Err(TsinkError::Other(format!(
            "{context}; visible marker recovery failed: {recovery_error}"
        ))),
    }
}

fn apply_compaction_replacement_marker(
    data_path: &Path,
    marker_path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<Option<CompactionOutcome>> {
    // A visible replacement after a prior parent-sync failure may still have an older durable
    // phase after a crash. Establish the currently visible marker as durable before interpreting
    // it or moving any source out of the segment namespace.
    crate::engine::fs_utils::sync_parent_dir(marker_path)?;
    let marker = parse_compaction_replacement_marker(data_path, marker_path)?;

    if marker.phase == CompactionReplacementPhase::Preparing {
        rollback_output_segments(&marker.output_segments, local_disk_budget)?;
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
            marker_path,
            local_disk_budget,
            crate::DiskCategory::Temporary,
        )?;
        return Ok(None);
    }

    for output in &marker.output_segments {
        validate_complete_segment(output)?;
    }

    let mut retirement_states = Vec::with_capacity(marker.source_segments.len());
    for (index, source) in marker.source_segments.iter().enumerate() {
        let retired = marker_owned_retired_source_path(data_path, marker_path, index)?;
        let source_metadata = entry_metadata(source)?;
        let retired_metadata = entry_metadata(&retired)?;
        let state = match (source_metadata, retired_metadata) {
            (Some(metadata), None)
                if metadata.file_type().is_dir()
                    && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
            {
                if marker.version == COMPACTION_REPLACEMENT_VERSION {
                    validate_complete_segment(source)?;
                } else {
                    validate_legacy_partial_segment_entries_no_follow(source)?;
                }
                SourceRetirementState::SourcePresent
            }
            (Some(_), None) => {
                return Err(TsinkError::DataCorruption(format!(
                    "compaction replacement source is link-like or not a directory: {}",
                    source.display()
                )));
            }
            (None, Some(metadata))
                if metadata.file_type().is_dir()
                    && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
            {
                SourceRetirementState::RetiredPresent
            }
            (None, None) => SourceRetirementState::Gone,
            (Some(_), Some(_)) => {
                return Err(TsinkError::DataCorruption(format!(
                    "compaction replacement source and its owned retirement path both exist: source={}, retired={}",
                    source.display(),
                    retired.display()
                )));
            }
            (None, Some(_)) => {
                return Err(TsinkError::DataCorruption(format!(
                    "compaction replacement retirement path is link-like or not a directory: {}",
                    retired.display()
                )));
            }
        };
        retirement_states.push((source.clone(), retired, state));
    }
    if marker.version == LEGACY_COMPACTION_REPLACEMENT_VERSION {
        for (_, retired, state) in &retirement_states {
            if *state == SourceRetirementState::RetiredPresent {
                validate_legacy_partial_segment_entries_no_follow(retired)?;
            }
        }
    }

    let has_visible_source = retirement_states
        .iter()
        .any(|(_, _, state)| *state == SourceRetirementState::SourcePresent);
    if has_visible_source {
        if marker.version == COMPACTION_REPLACEMENT_VERSION
            && retirement_states
                .iter()
                .any(|(_, _, state)| *state == SourceRetirementState::Gone)
        {
            return Err(TsinkError::DataCorruption(
                "compaction replacement has a missing source without owned retirement state while another source remains visible"
                    .to_string(),
            ));
        }
        // A prior pass cannot have begun recursive cleanup until every source name disappeared.
        // Therefore a marker-owned path alongside a still-visible source must remain a complete
        // segment; an incomplete one is corruption rather than retryable cleanup state.
        for (source, retired, state) in &retirement_states {
            if *state != SourceRetirementState::RetiredPresent {
                continue;
            }
            if marker.version == LEGACY_COMPACTION_REPLACEMENT_VERSION {
                continue;
            }
            let (expected_level, expected_segment_id) = segment_identity_from_root(source)?;
            validate_complete_segment_identity(retired, expected_level, expected_segment_id)?;
        }
    }

    // Move every validated source atomically out of the loader-visible segment namespace before
    // recursively deleting any bytes. If cleanup is interrupted, Ready recovery sees either the
    // intact source or this exact marker-owned path, never a partially deleted segment root.
    for (source, retired, state) in &retirement_states {
        if *state == SourceRetirementState::SourcePresent {
            retire_source_segment(source, retired, local_disk_budget)?;
        }
    }

    for (source, retired, state) in &retirement_states {
        if *state == SourceRetirementState::Gone {
            continue;
        }
        if entry_metadata(source)?.is_some() {
            return Err(TsinkError::Other(format!(
                "compaction source remained visible after retirement: {}",
                source.display()
            )));
        }
        match entry_metadata(retired)? {
            Some(metadata)
                if metadata.file_type().is_dir()
                    && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) => {}
            _ => {
                return Err(TsinkError::Other(format!(
                    "compaction source retirement path is not durably visible: {}",
                    retired.display()
                )));
            }
        }
    }

    for (_, retired, state) in &retirement_states {
        if *state == SourceRetirementState::Gone {
            continue;
        }
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
            retired,
            local_disk_budget,
            crate::DiskCategory::Temporary,
        )?;
    }

    let committed_outcome = CompactionOutcome {
        stats: CompactionRunStats {
            compacted: true,
            source_segments: marker.source_segments.len(),
            output_segments: marker.output_segments.len(),
            ..CompactionRunStats::default()
        },
        source_roots: marker.source_segments,
        output_roots: marker.output_segments,
    };
    let marker_removal = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
        marker_path,
        local_disk_budget,
        crate::DiskCategory::Temporary,
    );
    if let Err(removal_error) = marker_removal {
        match entry_metadata(marker_path) {
            Ok(Some(_)) => return Err(removal_error),
            Ok(None) => {}
            Err(probe_error) => {
                tracing::warn!(
                    marker = %marker_path.display(),
                    error = %removal_error,
                    probe_error = %probe_error,
                    "compaction replacement committed but final marker visibility probe failed"
                );
            }
        }
        // The filesystem mutation is already committed from the live catalog's perspective.
        // Retry durability once, but never discard the diff merely because the final unlink's
        // parent sync or budget reconciliation reported an error after the marker disappeared.
        if let Err(sync_error) = crate::engine::fs_utils::sync_parent_dir(marker_path) {
            tracing::warn!(
                marker = %marker_path.display(),
                error = %removal_error,
                retry_error = %sync_error,
                "compaction replacement committed with marker cleanup durability debt"
            );
        } else {
            tracing::warn!(
                marker = %marker_path.display(),
                error = %removal_error,
                "compaction replacement committed after retrying marker cleanup durability"
            );
        }
    }

    Ok(Some(committed_outcome))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceRetirementState {
    SourcePresent,
    RetiredPresent,
    Gone,
}

fn entry_metadata(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn marker_owned_retired_source_path(
    data_path: &Path,
    marker_path: &Path,
    index: usize,
) -> Result<PathBuf> {
    let expected_parent = compaction_replacement_dir(data_path);
    if marker_path.parent() != Some(expected_parent.as_path()) {
        return Err(TsinkError::DataCorruption(format!(
            "compaction replacement marker is outside its exact directory: {}",
            marker_path.display()
        )));
    }
    let marker_name = marker_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| is_compaction_replacement_marker_name(name))
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "compaction replacement marker has an invalid name: {}",
                marker_path.display()
            ))
        })?;
    Ok(expected_parent.join(format!(".retired-{marker_name}-{index:016x}")))
}

fn pending_source_retirement_entry_count(
    data_path: &Path,
    marker_path: &Path,
    source_segments: &[PathBuf],
) -> Result<u64> {
    let mut count = 0u64;
    for (index, source) in source_segments.iter().enumerate() {
        let retired = marker_owned_retired_source_path(data_path, marker_path, index)?;
        if entry_metadata(source)?.is_some() && entry_metadata(&retired)?.is_none() {
            count = count.checked_add(1).ok_or_else(|| {
                TsinkError::Other(
                    "compaction recovery retirement entry count exceeds the supported range"
                        .to_string(),
                )
            })?;
        }
    }
    Ok(count)
}

fn retire_source_segment(
    source: &Path,
    retired: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let rename_result = crate::engine::fs_utils::rename_and_sync_parents_budgeted_reclassify(
        source,
        retired,
        local_disk_budget,
        crate::DiskCategory::Segments,
        crate::DiskCategory::Temporary,
    );
    if let Err(rename_error) = rename_result {
        let source_metadata = entry_metadata(source)?;
        let retired_metadata = entry_metadata(retired)?;
        match (source_metadata, retired_metadata) {
            (None, Some(metadata))
                if metadata.file_type().is_dir()
                    && !crate::engine::fs_utils::is_link_or_reparse_point(&metadata) =>
            {
                crate::engine::fs_utils::sync_parent_dir(source)?;
                crate::engine::fs_utils::sync_parent_dir(retired)?;
                return Ok(());
            }
            (Some(_), None) => return Err(rename_error),
            (source_state, retired_state) => {
                return Err(TsinkError::Other(format!(
                    "compaction source retirement failed with indeterminate visibility: source={}, source_exists={}, retired={}, retired_exists={}, error={rename_error}",
                    source.display(),
                    source_state.is_some(),
                    retired.display(),
                    retired_state.is_some()
                )));
            }
        }
    }
    Ok(())
}

fn rollback_output_segments(
    output_segments: &[PathBuf],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    for segment in output_segments.iter().rev() {
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
            segment,
            local_disk_budget,
            crate::DiskCategory::Segments,
        )?;
    }
    Ok(())
}

fn cleanup_failed_preparing_compaction(
    marker_path: &Path,
    planned_outputs: &[PathBuf],
    primary: TsinkError,
) -> TsinkError {
    if let Err(rollback) = rollback_output_segments(planned_outputs, None) {
        // Keep the durable Preparing marker when cleanup is incomplete. Recovery can safely
        // retry every planned output without touching a source segment.
        return TsinkError::Other(format!(
            "compaction output write failed and planned-output rollback failed: write={primary}, rollback={rollback}"
        ));
    }
    if let Err(marker_cleanup) =
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(marker_path)
    {
        return TsinkError::Other(format!(
            "compaction output write failed and Preparing marker cleanup failed: write={primary}, marker_cleanup={marker_cleanup}"
        ));
    }
    primary
}

fn compaction_marker_payload_len(
    data_path: &Path,
    source_segments: &[PathBuf],
    output_segments: &[PathBuf],
    phase: CompactionReplacementPhase,
) -> Result<u64> {
    u64::try_from(
        compaction_replacement_marker_payload(data_path, source_segments, output_segments, phase)?
            .len(),
    )
    .map_err(|_| {
        TsinkError::Other(
            "compaction replacement marker exceeds the supported byte range".to_string(),
        )
    })
}

fn compaction_physical_peak_entry_count(
    data_path: &Path,
    target_level: u8,
    output_count: usize,
    source_count: usize,
) -> Result<u64> {
    // Each segment publication creates five files and one directory entry; the staging directory
    // is renamed to the final name and the two names never coexist. Rewriting Preparing to Ready
    // can transiently retain the published marker and its newly-synced temporary file together.
    // Atomic source retirement can also allocate one new marker-directory entry while the old
    // segment-parent directory block still occupies space.
    let output_entries = u64::try_from(output_count)
        .map_err(|_| {
            TsinkError::Other(
                "compaction output count exceeds the supported entry range".to_string(),
            )
        })?
        .checked_mul(6)
        .ok_or_else(|| {
            TsinkError::Other(
                "compaction output entry count exceeds the supported range".to_string(),
            )
        })?;

    let root = fs::canonicalize(data_path).map_err(|source| TsinkError::IoWithPath {
        path: data_path.to_path_buf(),
        source,
    })?;
    let mut directories = vec![root.join(COMPACTION_REPLACEMENT_DIR)];
    if output_count > 0 {
        directories.push(root.join("segments"));
        directories.push(root.join("segments").join(format!("L{target_level}")));
    }
    let mut missing = BTreeSet::new();
    for directory in directories {
        match fs::symlink_metadata(&directory) {
            Ok(metadata)
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_dir() =>
            {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "compaction directory path contains a link-like or non-directory entry: {}",
                    directory.display()
                )));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                missing.insert(directory);
            }
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: directory,
                    source,
                });
            }
        }
    }
    let missing_entries = u64::try_from(missing.len()).map_err(|_| {
        TsinkError::Other("compaction ancestry entry count exceeds the supported range".to_string())
    })?;
    let retired_entries = u64::try_from(source_count).map_err(|_| {
        TsinkError::Other("compaction source count exceeds the supported entry range".to_string())
    })?;
    output_entries
        .checked_add(2)
        .and_then(|entries| entries.checked_add(retired_entries))
        .and_then(|entries| entries.checked_add(missing_entries))
        .ok_or_else(|| {
            TsinkError::Other(
                "compaction physical entry count exceeds the supported range".to_string(),
            )
        })
}

fn retention_rewrite_physical_peak_entry_count(
    data_path: &Path,
    target_level: u8,
    output_count: usize,
) -> Result<u64> {
    if output_count == 0 {
        return Ok(0);
    }
    let output_entries = u64::try_from(output_count)
        .map_err(|_| {
            TsinkError::Other(
                "retention rewrite output count exceeds the supported entry range".to_string(),
            )
        })?
        .checked_mul(6)
        .ok_or_else(|| {
            TsinkError::Other(
                "retention rewrite output entry count exceeds the supported range".to_string(),
            )
        })?;
    let root = fs::canonicalize(data_path).map_err(|source| TsinkError::IoWithPath {
        path: data_path.to_path_buf(),
        source,
    })?;
    let directories = [
        root.join("segments"),
        root.join("segments").join(format!("L{target_level}")),
    ];
    let mut missing_entries = 0u64;
    for directory in directories {
        match fs::symlink_metadata(&directory) {
            Ok(metadata)
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_dir() =>
            {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "retention rewrite directory path contains a link-like or non-directory entry: {}",
                    directory.display()
                )));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                missing_entries = missing_entries.checked_add(1).ok_or_else(|| {
                    TsinkError::Other(
                        "retention rewrite ancestry entry count exceeds the supported range"
                            .to_string(),
                    )
                })?;
            }
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: directory,
                    source,
                });
            }
        }
    }
    output_entries.checked_add(missing_entries).ok_or_else(|| {
        TsinkError::Other(
            "retention rewrite physical entry count exceeds the supported range".to_string(),
        )
    })
}

#[cfg(test)]
pub(in crate::engine) fn finalize_pending_compaction_replacements(data_path: &Path) -> Result<()> {
    loop {
        let outcome = finalize_pending_compaction_replacements_with_disk_budget(data_path, None)?;
        if !outcome.stats.compacted {
            return Ok(());
        }
    }
}

pub(in crate::engine) fn finalize_pending_compaction_replacements_with_disk_budget(
    data_path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<CompactionOutcome> {
    let marker_dir = compaction_replacement_dir(data_path);
    match fs::symlink_metadata(&marker_dir) {
        Ok(metadata)
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_dir() =>
        {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement marker directory is link-like or not a directory: {}",
                marker_dir.display()
            )));
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CompactionOutcome::default());
        }
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: marker_dir,
                source,
            });
        }
    }
    let mut marker_paths = Vec::new();
    for entry in crate::engine::fs_utils::collect_directory_entries_bounded(
        &marker_dir,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        "compaction replacement marker recovery",
    )? {
        if !entry
            .file_name()
            .to_str()
            .is_some_and(is_compaction_replacement_marker_name)
        {
            continue;
        }
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "compaction replacement marker is link-like or not regular: {}",
                entry.path().display()
            )));
        }
        marker_paths.push(entry.path());
    }
    marker_paths.sort();

    for marker_path in marker_paths {
        let outcome = if let Some(budget) = local_disk_budget {
            if budget.governs_entry(&marker_path)? {
                crate::engine::fs_utils::sync_parent_dir(&marker_path)?;
                let marker = parse_compaction_replacement_marker(data_path, &marker_path)?;
                if marker.phase == CompactionReplacementPhase::Ready {
                    let retirement_entries = pending_source_retirement_entry_count(
                        data_path,
                        &marker_path,
                        &marker.source_segments,
                    )?;
                    let retirement_peak = retirement_entries
                        .checked_mul(budget.snapshot_restore_entry_staging_allowance_bytes()?)
                        .ok_or_else(|| {
                            TsinkError::Other(
                                "compaction recovery retirement peak exceeds the supported range"
                                    .to_string(),
                            )
                        })?;
                    budget.with_reconciled_recovery_reservation(
                        crate::DiskCategory::Temporary,
                        retirement_peak,
                        || apply_compaction_replacement_marker(data_path, &marker_path, None),
                    )?
                } else {
                    apply_compaction_replacement_marker(data_path, &marker_path, local_disk_budget)?
                }
            } else {
                apply_compaction_replacement_marker(data_path, &marker_path, None)?
            }
        } else {
            apply_compaction_replacement_marker(data_path, &marker_path, None)?
        };
        if let Some(outcome) = outcome {
            // A later marker may fail. Return each committed Ready diff immediately so runtime
            // callers cannot lose an already-unlinked marker's catalog update behind that error.
            return Ok(outcome);
        }
    }

    Ok(CompactionOutcome::default())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CompactionEmissionStats {
    output_segments: usize,
    output_chunks: usize,
    output_points: usize,
}

struct CompactionPreflight {
    peak_bytes: u64,
    emission: CompactionEmissionStats,
}

struct WrittenCompaction {
    emission: CompactionEmissionStats,
    output_roots: Vec<PathBuf>,
    replacement_applied: bool,
}

#[cfg(test)]
thread_local! {
    static INTERRUPT_AFTER_OUTPUT: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(super) struct CompactionOutputInterruptionGuard;

#[cfg(test)]
impl Drop for CompactionOutputInterruptionGuard {
    fn drop(&mut self) {
        INTERRUPT_AFTER_OUTPUT.with(|slot| slot.set(None));
    }
}

#[cfg(test)]
pub(super) fn interrupt_after_compaction_output(
    output_count: usize,
) -> CompactionOutputInterruptionGuard {
    INTERRUPT_AFTER_OUTPUT.with(|slot| slot.set(Some(output_count)));
    CompactionOutputInterruptionGuard
}

#[cfg(test)]
fn maybe_interrupt_after_compaction_output(output_count: usize) {
    INTERRUPT_AFTER_OUTPUT.with(|slot| {
        if slot.get() == Some(output_count) {
            slot.set(None);
            panic!("injected compaction interruption after output {output_count}");
        }
    });
}

#[cfg(not(test))]
fn maybe_interrupt_after_compaction_output(_output_count: usize) {}

impl Compactor {
    /// Stages the complete local retention-rewrite output group under one aggregate reservation.
    /// Subsequent tier copy/publication is a separate caller-owned durability and capacity phase.
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
        let aggregate_budget = match self.local_disk_budget.as_ref() {
            Some(budget) if budget.governs_entry(&self.data_path)? => Some(Arc::clone(budget)),
            _ => None,
        };

        let written = if apply_replacement {
            let preflight = self.measure_compaction_operation_peak(
                target_level,
                &series,
                &chunks_by_series,
                tombstones,
                retention_cutoff,
                output_segment_point_budget,
                &registry,
                source_wal_highwater,
                &source_roots,
                aggregate_budget.as_ref(),
            )?;
            let operation = || {
                let planned_outputs = self.allocate_compaction_outputs(
                    target_level,
                    preflight.emission.output_segments,
                )?;
                self.write_planned_compaction(
                    target_level,
                    &series,
                    &chunks_by_series,
                    tombstones,
                    retention_cutoff,
                    output_segment_point_budget,
                    &registry,
                    source_wal_highwater,
                    &source_roots,
                    &planned_outputs,
                    preflight.emission,
                )
            };
            if let Some(budget) = aggregate_budget {
                budget.with_reconciled_maintenance_reservation(
                    self.output_disk_category,
                    preflight.peak_bytes,
                    operation,
                )?
            } else {
                operation()?
            }
        } else if let Some(budget) = aggregate_budget {
            let preflight = self.measure_retention_rewrite_peak(
                target_level,
                &series,
                &chunks_by_series,
                tombstones,
                retention_cutoff,
                output_segment_point_budget,
                &registry,
                source_wal_highwater,
                &budget,
            )?;
            budget.with_reconciled_maintenance_reservation(
                self.output_disk_category,
                preflight.peak_bytes,
                || {
                    self.write_retention_outputs(
                        target_level,
                        &series,
                        &chunks_by_series,
                        tombstones,
                        retention_cutoff,
                        output_segment_point_budget,
                        &registry,
                        source_wal_highwater,
                        None,
                    )
                },
            )?
        } else {
            self.write_retention_outputs(
                target_level,
                &series,
                &chunks_by_series,
                tombstones,
                retention_cutoff,
                output_segment_point_budget,
                &registry,
                source_wal_highwater,
                self.local_disk_budget.clone(),
            )?
        };

        Ok(CompactionOutcome {
            stats: CompactionRunStats {
                compacted: written.replacement_applied || written.emission.output_segments > 0,
                source_level: None,
                target_level: Some(target_level),
                source_segments,
                output_segments: written.emission.output_segments,
                source_chunks,
                output_chunks: written.emission.output_chunks,
                source_points,
                output_points: written.emission.output_points,
                ..CompactionRunStats::default()
            },
            source_roots,
            output_roots: written.output_roots,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn measure_compaction_operation_peak(
        &self,
        target_level: u8,
        series: &[PersistedSeries],
        chunks_by_series: &SeriesChunkRefs<'_>,
        tombstones: &TombstoneMap,
        retention_cutoff: Option<i64>,
        output_segment_point_budget: usize,
        registry: &SeriesRegistry,
        source_wal_highwater: WalHighWatermark,
        source_roots: &[PathBuf],
        aggregate_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<CompactionPreflight> {
        let mut output_bytes = 0u64;
        let emission = for_each_compacted_output(
            series,
            chunks_by_series,
            self.point_cap,
            tombstones,
            retention_cutoff,
            output_segment_point_budget,
            |pending_chunks| {
                // Segment ids occupy a fixed-width field in both the binary manifest and the
                // directory name, so id zero yields the exact byte count for every planned id.
                let writer = SegmentWriter::new(&self.data_path, target_level, 0)?;
                let segment_bytes = writer.measure_segment_bytes_with_wal_highwater(
                    registry,
                    pending_chunks,
                    source_wal_highwater,
                )?;
                output_bytes = output_bytes.checked_add(segment_bytes).ok_or_else(|| {
                    TsinkError::Other(
                        "compaction output peak exceeds the supported byte range".to_string(),
                    )
                })?;
                Ok(())
            },
        )?;

        let mut placeholder_output_roots = Vec::with_capacity(emission.output_segments);
        for index in 0..emission.output_segments {
            let segment_id = u64::try_from(index).map_err(|_| {
                TsinkError::Other(
                    "compaction output count exceeds the supported id range".to_string(),
                )
            })?;
            placeholder_output_roots.push(
                SegmentWriter::new(&self.data_path, target_level, segment_id)?
                    .layout()
                    .root
                    .clone(),
            );
        }
        let preparing_marker_bytes = compaction_marker_payload_len(
            &self.data_path,
            source_roots,
            &placeholder_output_roots,
            CompactionReplacementPhase::Preparing,
        )?;
        let ready_marker_bytes = compaction_marker_payload_len(
            &self.data_path,
            source_roots,
            &placeholder_output_roots,
            CompactionReplacementPhase::Ready,
        )?;
        let marker_rewrite_peak = preparing_marker_bytes
            .checked_add(ready_marker_bytes)
            .ok_or_else(|| {
                TsinkError::Other(
                    "compaction marker peak exceeds the supported byte range".to_string(),
                )
            })?;
        let logical_peak_bytes =
            output_bytes
                .checked_add(marker_rewrite_peak)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "compaction operation peak exceeds the supported byte range".to_string(),
                    )
                })?;
        let peak_bytes = if let Some(budget) = aggregate_budget {
            let entry_count = compaction_physical_peak_entry_count(
                &self.data_path,
                target_level,
                emission.output_segments,
                source_roots.len(),
            )?;
            let allowance = budget.snapshot_restore_entry_staging_allowance_bytes()?;
            let entry_bytes = entry_count.checked_mul(allowance).ok_or_else(|| {
                TsinkError::Other(
                    "compaction filesystem-entry allowance exceeds the supported byte range"
                        .to_string(),
                )
            })?;
            logical_peak_bytes.checked_add(entry_bytes).ok_or_else(|| {
                TsinkError::Other(
                    "compaction physical peak exceeds the supported byte range".to_string(),
                )
            })?
        } else {
            logical_peak_bytes
        };

        Ok(CompactionPreflight {
            peak_bytes,
            emission,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn measure_retention_rewrite_peak(
        &self,
        target_level: u8,
        series: &[PersistedSeries],
        chunks_by_series: &SeriesChunkRefs<'_>,
        tombstones: &TombstoneMap,
        retention_cutoff: Option<i64>,
        output_segment_point_budget: usize,
        registry: &SeriesRegistry,
        source_wal_highwater: WalHighWatermark,
        budget: &Arc<crate::LocalDiskBudget>,
    ) -> Result<CompactionPreflight> {
        let mut output_bytes = 0u64;
        let emission = for_each_compacted_output(
            series,
            chunks_by_series,
            self.point_cap,
            tombstones,
            retention_cutoff,
            output_segment_point_budget,
            |pending_chunks| {
                let writer = SegmentWriter::new(&self.data_path, target_level, 0)?;
                let segment_bytes = writer.measure_segment_bytes_with_wal_highwater(
                    registry,
                    pending_chunks,
                    source_wal_highwater,
                )?;
                output_bytes = output_bytes.checked_add(segment_bytes).ok_or_else(|| {
                    TsinkError::Other(
                        "retention rewrite output peak exceeds the supported byte range"
                            .to_string(),
                    )
                })?;
                Ok(())
            },
        )?;
        let entry_count = retention_rewrite_physical_peak_entry_count(
            &self.data_path,
            target_level,
            emission.output_segments,
        )?;
        let entry_bytes = entry_count
            .checked_mul(budget.snapshot_restore_entry_staging_allowance_bytes()?)
            .ok_or_else(|| {
                TsinkError::Other(
                    "retention rewrite filesystem-entry allowance exceeds the supported byte range"
                        .to_string(),
                )
            })?;
        let peak_bytes = output_bytes.checked_add(entry_bytes).ok_or_else(|| {
            TsinkError::Other(
                "retention rewrite physical peak exceeds the supported byte range".to_string(),
            )
        })?;
        Ok(CompactionPreflight {
            peak_bytes,
            emission,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn write_planned_compaction(
        &self,
        target_level: u8,
        series: &[PersistedSeries],
        chunks_by_series: &SeriesChunkRefs<'_>,
        tombstones: &TombstoneMap,
        retention_cutoff: Option<i64>,
        output_segment_point_budget: usize,
        registry: &SeriesRegistry,
        source_wal_highwater: WalHighWatermark,
        source_roots: &[PathBuf],
        planned_outputs: &[(u64, PathBuf)],
        expected_emission: CompactionEmissionStats,
    ) -> Result<WrittenCompaction> {
        let output_roots = planned_outputs
            .iter()
            .map(|(_, root)| root.clone())
            .collect::<Vec<_>>();
        // The compaction namespace is serialized by the engine maintenance lock. Establish that
        // every final name is absent before durable Preparing intent makes those exact names
        // transaction-owned; never let rollback recursively remove an older unknown collision.
        for output_root in &output_roots {
            match fs::symlink_metadata(output_root) {
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: output_root.clone(),
                        source,
                    });
                }
                Ok(_) => {
                    return Err(TsinkError::InvalidConfiguration(format!(
                        "planned compaction output already exists: {}",
                        output_root.display()
                    )));
                }
            }
        }
        let marker_path = replacement_marker_path(&self.data_path)?;
        if let Err(failure) = write_compaction_replacement_marker_at(
            &self.data_path,
            source_roots,
            &output_roots,
            CompactionReplacementPhase::Preparing,
            &marker_path,
            MarkerWriteMode::CreateNew,
        ) {
            recover_preparing_marker_publication_failure(
                &self.data_path,
                source_roots,
                &output_roots,
                failure,
            )?;
        }

        let mut output_index = 0usize;
        let write_outputs_result = for_each_compacted_output(
            series,
            chunks_by_series,
            self.point_cap,
            tombstones,
            retention_cutoff,
            output_segment_point_budget,
            |pending_chunks| {
                let (segment_id, planned_root) =
                    planned_outputs.get(output_index).ok_or_else(|| {
                        TsinkError::Other(
                            "compaction emitted more outputs than its durable plan".to_string(),
                        )
                    })?;
                let output_root = self.flush_compacted_segment(
                    target_level,
                    *segment_id,
                    registry,
                    pending_chunks,
                    source_wal_highwater,
                    None,
                )?;
                if output_root != *planned_root {
                    return Err(TsinkError::Other(format!(
                        "compaction output root diverged from durable plan: planned={}, actual={}",
                        planned_root.display(),
                        output_root.display()
                    )));
                }
                output_index = output_index.checked_add(1).ok_or_else(|| {
                    TsinkError::Other(
                        "compaction output count exceeds the supported range".to_string(),
                    )
                })?;
                maybe_interrupt_after_compaction_output(output_index);
                Ok(())
            },
        );
        let emission = match write_outputs_result {
            Ok(emission) => emission,
            Err(err) => {
                return Err(cleanup_failed_preparing_compaction(
                    &marker_path,
                    &output_roots,
                    err,
                ));
            }
        };

        if emission != expected_emission || output_index != planned_outputs.len() {
            let mismatch = TsinkError::Other(format!(
                "compaction output changed after capacity preflight: expected {expected_emission:?}, emitted {emission:?}, planned_roots={}, written_roots={output_index}",
                planned_outputs.len()
            ));
            return Err(cleanup_failed_preparing_compaction(
                &marker_path,
                &output_roots,
                mismatch,
            ));
        }

        if let Err(failure) = write_compaction_replacement_marker_at(
            &self.data_path,
            source_roots,
            &output_roots,
            CompactionReplacementPhase::Ready,
            &marker_path,
            MarkerWriteMode::ReplaceExisting,
        ) {
            resolve_ready_marker_publication_failure(
                &self.data_path,
                source_roots,
                &output_roots,
                failure,
            )?;
            return Ok(WrittenCompaction {
                emission,
                output_roots,
                replacement_applied: true,
            });
        }
        let _ = apply_compaction_replacement_marker(&self.data_path, &marker_path, None)?;

        Ok(WrittenCompaction {
            emission,
            output_roots,
            replacement_applied: true,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn write_retention_outputs(
        &self,
        target_level: u8,
        series: &[PersistedSeries],
        chunks_by_series: &SeriesChunkRefs<'_>,
        tombstones: &TombstoneMap,
        retention_cutoff: Option<i64>,
        output_segment_point_budget: usize,
        registry: &SeriesRegistry,
        source_wal_highwater: WalHighWatermark,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Result<WrittenCompaction> {
        let mut output_roots = Vec::new();
        let emission = for_each_compacted_output(
            series,
            chunks_by_series,
            self.point_cap,
            tombstones,
            retention_cutoff,
            output_segment_point_budget,
            |pending_chunks| {
                let segment_id = self.allocate_compaction_output_id()?;
                let output_root = self.flush_compacted_segment(
                    target_level,
                    segment_id,
                    registry,
                    pending_chunks,
                    source_wal_highwater,
                    local_disk_budget.clone(),
                )?;
                output_roots.push(output_root);
                Ok(())
            },
        );
        let emission = match emission {
            Ok(emission) => emission,
            Err(err) => {
                if let Err(rollback_err) =
                    rollback_output_segments(&output_roots, local_disk_budget.as_ref())
                {
                    return Err(TsinkError::Other(format!(
                        "retention output write failed and rollback failed: write={err}, rollback={rollback_err}"
                    )));
                }
                return Err(err);
            }
        };
        Ok(WrittenCompaction {
            emission,
            output_roots,
            replacement_applied: false,
        })
    }

    fn allocate_compaction_output_id(&self) -> Result<u64> {
        match &self.next_segment_id {
            Some(next_segment_id) => Ok(next_segment_id.fetch_add(1, Ordering::SeqCst)),
            None => Ok(load_segments_runtime_strict(&self.data_path)?.next_segment_id),
        }
    }

    fn allocate_compaction_outputs(
        &self,
        target_level: u8,
        output_count: usize,
    ) -> Result<Vec<(u64, PathBuf)>> {
        let count = u64::try_from(output_count).map_err(|_| {
            TsinkError::Other("compaction output count exceeds the supported range".to_string())
        })?;
        let first = match &self.next_segment_id {
            Some(next_segment_id) => loop {
                let current = next_segment_id.load(Ordering::SeqCst);
                let next = current.checked_add(count).ok_or_else(|| {
                    TsinkError::Other("compaction segment id range overflow".to_string())
                })?;
                match next_segment_id.compare_exchange(
                    current,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break current,
                    Err(_) => continue,
                }
            },
            None => load_segments_runtime_strict(&self.data_path)?.next_segment_id,
        };
        (0..count)
            .map(|offset| {
                let segment_id = first.checked_add(offset).ok_or_else(|| {
                    TsinkError::Other("compaction segment id range overflow".to_string())
                })?;
                let root = SegmentWriter::new(&self.data_path, target_level, segment_id)?
                    .layout()
                    .root
                    .clone();
                Ok((segment_id, root))
            })
            .collect()
    }

    fn flush_compacted_segment(
        &self,
        target_level: u8,
        segment_id: u64,
        registry: &SeriesRegistry,
        chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
        wal_highwater: WalHighWatermark,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Result<PathBuf> {
        if chunks_by_series.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot flush empty compacted segment".to_string(),
            ));
        }

        let writer = SegmentWriter::new_with_disk_budget_and_category(
            &self.data_path,
            target_level,
            segment_id,
            local_disk_budget,
            crate::DiskReservationKind::Maintenance,
            self.output_disk_category,
        )?;
        writer.write_segment_with_wal_highwater(registry, chunks_by_series, wal_highwater)?;
        Ok(writer.layout().root.clone())
    }
}

#[allow(clippy::too_many_arguments)]
fn for_each_compacted_output<F>(
    series: &[PersistedSeries],
    chunks_by_series: &SeriesChunkRefs<'_>,
    point_cap: usize,
    tombstones: &TombstoneMap,
    retention_cutoff: Option<i64>,
    output_segment_point_budget: usize,
    mut emit_output: F,
) -> Result<CompactionEmissionStats>
where
    F: FnMut(&HashMap<SeriesId, Vec<Chunk>>) -> Result<()>,
{
    let mut pending_chunks = HashMap::<SeriesId, Vec<Chunk>>::new();
    let mut pending_points = 0usize;
    let mut emission = CompactionEmissionStats::default();

    for series_def in series {
        let series_id = series_def.series_id;
        let Some(chunks) = chunks_by_series.get(&series_id) else {
            continue;
        };
        let tombstone_ranges = tombstones.get(&series_id).map(|ranges| ranges.as_slice());

        stream_merge_series_chunks(
            series_id,
            chunks,
            point_cap,
            tombstone_ranges,
            retention_cutoff,
            |chunk| {
                emission.output_chunks =
                    emission.output_chunks.checked_add(1).ok_or_else(|| {
                        TsinkError::Other(
                            "compaction output chunk count exceeds the supported range".to_string(),
                        )
                    })?;
                let chunk_points = chunk.header.point_count as usize;
                emission.output_points = emission
                    .output_points
                    .checked_add(chunk_points)
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "compaction output point count exceeds the supported range".to_string(),
                        )
                    })?;
                pending_points = pending_points.checked_add(chunk_points).ok_or_else(|| {
                    TsinkError::Other(
                        "compaction pending point count exceeds the supported range".to_string(),
                    )
                })?;
                pending_chunks.entry(series_id).or_default().push(chunk);
                if pending_points >= output_segment_point_budget {
                    emit_output(&pending_chunks)?;
                    pending_chunks.clear();
                    pending_points = 0;
                    emission.output_segments =
                        emission.output_segments.checked_add(1).ok_or_else(|| {
                            TsinkError::Other(
                                "compaction output segment count exceeds the supported range"
                                    .to_string(),
                            )
                        })?;
                }
                Ok(())
            },
        )?;
    }

    if !pending_chunks.is_empty() {
        emit_output(&pending_chunks)?;
        emission.output_segments = emission.output_segments.checked_add(1).ok_or_else(|| {
            TsinkError::Other(
                "compaction output segment count exceeds the supported range".to_string(),
            )
        })?;
    }

    Ok(emission)
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
        wal_lowwater: WalHighWatermark::default(),
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
