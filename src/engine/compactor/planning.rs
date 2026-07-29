use std::collections::BTreeSet;
use std::fs::File;
use std::io;

use super::*;

impl Compactor {
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        self.reset_background_compaction_recovery_cursor();
        let recovered = finalize_pending_compaction_replacements_with_disk_budget(
            &self.data_path,
            self.local_disk_budget.as_ref(),
        )?;
        if recovered.stats.compacted {
            // Return recovered Ready changes before planning more work so the caller can apply the
            // exact source/output diff to its live persisted catalog.
            return Ok(recovered);
        }
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;
        self.compact_once_with_changes_after_recovery(&tombstones)
    }

    /// Engine-integrated compaction uses the already-accounted authoritative tombstone map.
    /// Standalone `Compactor` callers retain the durable-file loading behavior above.
    pub(in crate::engine) fn compact_once_with_changes_using_tombstones(
        &self,
        tombstones: &TombstoneMap,
    ) -> Result<CompactionOutcome> {
        self.reset_background_compaction_recovery_cursor();
        let recovered = finalize_pending_compaction_replacements_with_disk_budget(
            &self.data_path,
            self.local_disk_budget.as_ref(),
        )?;
        if recovered.stats.compacted {
            return Ok(recovered);
        }
        self.compact_once_with_changes_after_recovery(tombstones)
    }

    /// One finite engine-background pass. Any admitted recovery namespace entry or marker owns
    /// the wake; regular planning starts only after a retained scan proves there is no pending
    /// replacement.
    pub(in crate::engine) fn compact_background_once_with_changes(
        &self,
    ) -> Result<CompactionOutcome> {
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;
        self.compact_background_once_with_changes_using_tombstones(&tombstones)
    }

    /// Engine-integrated finite background compaction uses the authoritative tombstone map.
    pub(in crate::engine) fn compact_background_once_with_changes_using_tombstones(
        &self,
        tombstones: &TombstoneMap,
    ) -> Result<CompactionOutcome> {
        let recovery = {
            let mut state = self
                .planning_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            execution::next_background_compaction_replacement_bounded(
                &mut state.background_recovery,
                &self.data_path,
                self.local_disk_budget.as_ref(),
                self.pass_limits,
            )?
        };
        match recovery {
            BackgroundCompactionRecoveryStep::NoPending => {
                self.compact_once_with_changes_after_recovery(tombstones)
            }
            BackgroundCompactionRecoveryStep::Ready(outcome) => Ok(outcome),
            BackgroundCompactionRecoveryStep::AllowanceExhausted
            | BackgroundCompactionRecoveryStep::NamespaceEntryConsumed
            | BackgroundCompactionRecoveryStep::PreparingRolledBack => {
                Ok(CompactionOutcome::default())
            }
        }
    }

    fn reset_background_compaction_recovery_cursor(&self) {
        let mut state = self
            .planning_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.background_recovery.reset();
    }

    fn compact_once_with_changes_after_recovery(
        &self,
        tombstones: &TombstoneMap,
    ) -> Result<CompactionOutcome> {
        let mut planning_stats = CompactionRunStats::default();
        let start_level = self.take_next_planning_level();
        let level_plans = [
            (CompactionLevel::L0, CompactionLevel::L1, self.l0_trigger),
            (CompactionLevel::L1, CompactionLevel::L2, self.l1_trigger),
        ];

        for offset in 0..level_plans.len() {
            let (source, target, trigger) = level_plans[(start_level + offset) % level_plans.len()];
            if let Some(mut outcome) =
                self.try_compact_level(source, target, trigger, tombstones, &mut planning_stats)?
            {
                copy_planning_stats(&mut outcome.stats, &planning_stats);
                return Ok(outcome);
            }
        }

        Ok(CompactionOutcome {
            stats: planning_stats,
            ..CompactionOutcome::default()
        })
    }

    fn take_next_planning_level(&self) -> usize {
        let mut state = self
            .planning_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let level = state.next_level % state.levels.len();
        state.next_level = (level + 1) % state.levels.len();
        level
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
        tombstones: &TombstoneMap,
        planning_stats: &mut CompactionRunStats,
    ) -> Result<Option<CompactionOutcome>> {
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);
        let candidates =
            self.discover_level_candidates(source_level, count_trigger, planning_stats)?;
        if candidates.len() < 2 {
            return Ok(None);
        }

        let should_compact = candidates.len() >= count_trigger || has_time_overlap(&candidates);
        if !should_compact {
            return Ok(None);
        }

        let max_segments = self
            .pass_limits
            .max_source_segments
            .min(DEFAULT_SOURCE_WINDOW_SEGMENTS);
        let window = select_compaction_window(&candidates, count_trigger, max_segments);
        if window.len() < 2 {
            planning_stats.planning_backlog_observed = true;
            planning_stats.planning_budget_exhausted = true;
            return Ok(None);
        }
        if candidates.len() > window.len() {
            planning_stats.planning_backlog_observed = true;
        }

        let selected_roots = window
            .iter()
            .map(|candidate| candidate.root.clone())
            .collect::<Vec<_>>();
        let Some(source_bytes) = self.preflight_source_window(&window, planning_stats)? else {
            // Drop only the in-memory planning entries. The durable roots remain untouched and
            // are rediscovered after the fair directory cursor has visited later candidates.
            self.forget_level_candidates(source_level, &selected_roots);
            return Ok(None);
        };
        planning_stats.planning_source_bytes = planning_stats
            .planning_source_bytes
            .saturating_add(source_bytes);

        let mut loaded = Vec::with_capacity(window.len());
        let mut disappeared = Vec::new();
        for candidate in &window {
            match crate::engine::segment::load_segment(&candidate.root) {
                Ok(segment) => {
                    if segment.manifest != candidate.manifest {
                        return Err(segment_validation_error(
                            &candidate.root,
                            SegmentValidationContext::Compaction,
                            "segment manifest changed between bounded planning and source load",
                        ));
                    }
                    loaded.push(segment);
                }
                Err(err) if is_not_found_error(&err) => {
                    disappeared.push(candidate.root.clone());
                }
                Err(TsinkError::DataCorruption(details)) => {
                    return Err(segment_validation_error(
                        &candidate.root,
                        SegmentValidationContext::Compaction,
                        &details,
                    ));
                }
                Err(err) => return Err(err),
            }
        }
        if !disappeared.is_empty() {
            self.forget_level_candidates(source_level, &disappeared);
        }
        if loaded.len() < 2 {
            return Ok(None);
        }

        let loaded_refs = loaded.iter().collect::<Vec<_>>();
        let mut outcome = self.compact_segments(target_level, &loaded_refs, tombstones)?;
        outcome.stats.compacted = true;
        outcome.stats.source_level = Some(source_level);
        outcome.stats.target_level = Some(target_level);
        self.forget_level_candidates(source_level, &selected_roots);
        Ok(Some(outcome))
    }

    fn discover_level_candidates(
        &self,
        level: u8,
        count_trigger: usize,
        stats: &mut CompactionRunStats,
    ) -> Result<Vec<SegmentCandidate>> {
        let level_index = usize::from(level);
        debug_assert!(level_index < 2);
        let mut state = self
            .planning_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cursor = &mut state.levels[level_index];
        let cache_limit = DEFAULT_SOURCE_WINDOW_SEGMENTS.max(count_trigger).max(2);
        let mut evicted = false;

        if cursor.entries.is_none() {
            cursor.entries = self.open_level_directory(level)?;
        }

        let mut reached_end = false;
        while stats.planning_directory_entries_inspected < self.pass_limits.max_directory_entries
            && stats.planning_manifests_inspected < self.pass_limits.max_manifest_inspections
        {
            let Some(entries) = cursor.entries.as_mut() else {
                reached_end = true;
                break;
            };
            let Some(entry) = entries.next() else {
                cursor.entries = None;
                reached_end = true;
                break;
            };
            stats.planning_directory_entries_inspected =
                stats.planning_directory_entries_inspected.saturating_add(1);
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            if !file_type.is_dir()
                || !entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("seg-"))
            {
                continue;
            }

            let root = entry.path();
            stats.planning_manifests_inspected =
                stats.planning_manifests_inspected.saturating_add(1);
            let fingerprint = match crate::engine::segment::read_segment_manifest_fingerprint(&root)
            {
                Ok(fingerprint) => fingerprint,
                Err(err) if is_not_found_error(&err) => continue,
                Err(TsinkError::DataCorruption(details)) => {
                    return Err(segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        &details,
                    ));
                }
                Err(err) => return Err(err),
            };
            if fingerprint.manifest.level != level {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &format!(
                        "segment directory level mismatch: stored under L{level}, manifest says L{}",
                        fingerprint.manifest.level
                    ),
                ));
            }
            let persisted_file_bytes = fingerprint.files.iter().try_fold(0u64, |sum, file| {
                sum.checked_add(file.file_len).ok_or_else(|| {
                    segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        "segment manifest file byte total overflows u64",
                    )
                })
            })?;
            let candidate = SegmentCandidate {
                root: root.clone(),
                manifest: fingerprint.manifest,
                persisted_file_bytes,
                persisted_chunks_file_bytes: fingerprint.files[0].file_len,
            };
            if let Some(existing) = cursor
                .candidates
                .iter_mut()
                .find(|existing| existing.root == root)
            {
                *existing = candidate;
            } else {
                cursor.candidates.push_back(candidate);
                while cursor.candidates.len() > cache_limit {
                    cursor.candidates.pop_front();
                    evicted = true;
                }
            }
        }

        let directory_limit_reached =
            stats.planning_directory_entries_inspected >= self.pass_limits.max_directory_entries;
        let manifest_limit_reached =
            stats.planning_manifests_inspected >= self.pass_limits.max_manifest_inspections;
        if !reached_end && (directory_limit_reached || manifest_limit_reached) {
            stats.planning_budget_exhausted = true;
            stats.planning_backlog_observed = true;
        }
        if evicted {
            stats.planning_backlog_observed = true;
        }
        stats.planning_candidates_observed = stats
            .planning_candidates_observed
            .saturating_add(cursor.candidates.len());
        let mut candidates = cursor.candidates.iter().cloned().collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| candidate.manifest.segment_id);
        Ok(candidates)
    }

    fn open_level_directory(&self, level: u8) -> Result<Option<fs::ReadDir>> {
        let segments_root = self.data_path.join("segments");
        let level_root = segments_root.join(format!("L{level}"));
        for path in [&self.data_path, &segments_root, &level_root] {
            let metadata = match fs::symlink_metadata(path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err.into()),
            };
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_dir()
            {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!(
                        "compaction namespace is link-like or not a directory: {}",
                        path.display()
                    ),
                )
                .into());
            }
        }
        match fs::read_dir(level_root) {
            Ok(entries) => Ok(Some(entries)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn preflight_source_window(
        &self,
        window: &[&SegmentCandidate],
        stats: &mut CompactionRunStats,
    ) -> Result<Option<u64>> {
        let mut chunks = 0usize;
        let mut points = 0usize;
        let mut modeled_bytes = 0u64;
        for candidate in window {
            chunks = chunks.saturating_add(candidate.manifest.chunk_count);
            points = points.saturating_add(candidate.manifest.point_count);
            modeled_bytes = modeled_bytes.saturating_add(candidate.persisted_file_bytes);
        }
        if window.len() > self.pass_limits.max_source_segments
            || chunks > self.pass_limits.max_source_chunks
            || points > self.pass_limits.max_source_points
            || modeled_bytes > self.pass_limits.max_decoded_bytes
        {
            stats.planning_backlog_observed = true;
            stats.planning_budget_exhausted = true;
            return Ok(None);
        }

        for candidate in window {
            let chunks_path = candidate.root.join("chunks.bin");
            let file = match File::open(&chunks_path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    match fs::symlink_metadata(&candidate.root) {
                        Err(root_err) if root_err.kind() == io::ErrorKind::NotFound => {
                            return Ok(None);
                        }
                        Ok(_) => {
                            return Err(segment_validation_error(
                                &candidate.root,
                                SegmentValidationContext::Compaction,
                                "segment is missing chunks.bin during bounded source preflight",
                            ));
                        }
                        Err(root_err) => return Err(root_err.into()),
                    }
                }
                Err(err) => return Err(err.into()),
            };
            let current_len = file.metadata()?.len();
            modeled_bytes = modeled_bytes
                .saturating_sub(candidate.persisted_chunks_file_bytes)
                .saturating_add(current_len);
            if current_len == 0 {
                return Err(segment_validation_error(
                    &candidate.root,
                    SegmentValidationContext::Compaction,
                    "segment chunks.bin is empty during bounded source preflight",
                ));
            }
            if modeled_bytes > self.pass_limits.max_decoded_bytes {
                stats.planning_backlog_observed = true;
                stats.planning_budget_exhausted = true;
                return Ok(None);
            }
            let mapped = crate::mmap::create_mmap(file).map_err(|err| TsinkError::MemoryMap {
                path: chunks_path.clone(),
                details: err.to_string(),
            })?;
            let decoded =
                crate::engine::segment::decoded_chunks_file_payload_bytes(mapped.as_slice())?;
            modeled_bytes = modeled_bytes.saturating_add(decoded as u64);
            if modeled_bytes > self.pass_limits.max_decoded_bytes {
                stats.planning_backlog_observed = true;
                stats.planning_budget_exhausted = true;
                return Ok(None);
            }
        }

        let modeled_points =
            (points as u64).saturating_mul(std::mem::size_of::<ChunkPoint>().max(1) as u64);
        modeled_bytes = modeled_bytes.saturating_add(modeled_points);
        if modeled_bytes > self.pass_limits.max_decoded_bytes {
            stats.planning_backlog_observed = true;
            stats.planning_budget_exhausted = true;
            return Ok(None);
        }
        Ok(Some(modeled_bytes))
    }

    fn forget_level_candidates(&self, level: u8, roots: &[PathBuf]) {
        let roots = roots.iter().collect::<BTreeSet<_>>();
        let mut state = self
            .planning_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.levels[usize::from(level)]
            .candidates
            .retain(|candidate| !roots.contains(&candidate.root));
    }
}

fn copy_planning_stats(target: &mut CompactionRunStats, source: &CompactionRunStats) {
    target.planning_directory_entries_inspected = source.planning_directory_entries_inspected;
    target.planning_manifests_inspected = source.planning_manifests_inspected;
    target.planning_candidates_observed = source.planning_candidates_observed;
    target.planning_source_bytes = source.planning_source_bytes;
    target.planning_backlog_observed = source.planning_backlog_observed;
    target.planning_budget_exhausted = source.planning_budget_exhausted;
}

fn has_time_overlap(segments: &[SegmentCandidate]) -> bool {
    let mut ranges = segments
        .iter()
        .filter_map(|segment| {
            Some((
                segment.manifest.min_ts?,
                segment.manifest.max_ts?,
                segment.manifest.segment_id,
            ))
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return false;
    }
    ranges.sort_by_key(|(min_ts, _, segment_id)| (*min_ts, *segment_id));
    let mut current_max = ranges[0].1;
    for (min_ts, max_ts, _) in ranges.into_iter().skip(1) {
        if min_ts <= current_max {
            return true;
        }
        current_max = current_max.max(max_ts);
    }
    false
}

fn select_compaction_window(
    segments: &[SegmentCandidate],
    count_trigger: usize,
    max_segments: usize,
) -> Vec<&SegmentCandidate> {
    if max_segments < 2 {
        return Vec::new();
    }
    if let Some(indexes) = overlapping_window_indexes(segments, max_segments) {
        return indexes
            .into_iter()
            .filter_map(|index| segments.get(index))
            .collect();
    }
    let window_len = count_trigger.max(2).min(max_segments).min(segments.len());
    segments.iter().take(window_len).collect()
}

#[derive(Debug, Clone, Copy)]
struct SegmentTimeRange {
    index: usize,
    segment_id: u64,
    min_ts: i64,
    max_ts: i64,
}

fn overlapping_window_indexes(
    segments: &[SegmentCandidate],
    max_segments: usize,
) -> Option<Vec<usize>> {
    let mut ranges = segments
        .iter()
        .enumerate()
        .filter_map(|(index, segment)| {
            Some(SegmentTimeRange {
                index,
                segment_id: segment.manifest.segment_id,
                min_ts: segment.manifest.min_ts?,
                max_ts: segment.manifest.max_ts?,
            })
        })
        .collect::<Vec<_>>();
    if ranges.len() < 2 {
        return None;
    }
    ranges.sort_by_key(|range| (range.min_ts, range.segment_id));

    let mut cluster_start = 0usize;
    let mut cluster_max = ranges[0].max_ts;
    for idx in 1..ranges.len() {
        if ranges[idx].min_ts <= cluster_max {
            cluster_max = cluster_max.max(ranges[idx].max_ts);
            continue;
        }
        if idx.saturating_sub(cluster_start) >= 2 {
            return Some(select_cluster_indexes(
                &ranges[cluster_start..idx],
                max_segments,
            ));
        }
        cluster_start = idx;
        cluster_max = ranges[idx].max_ts;
    }
    if ranges.len().saturating_sub(cluster_start) >= 2 {
        return Some(select_cluster_indexes(
            &ranges[cluster_start..],
            max_segments,
        ));
    }
    None
}

fn select_cluster_indexes(cluster: &[SegmentTimeRange], max_segments: usize) -> Vec<usize> {
    let mut indexes = cluster.iter().map(|range| range.index).collect::<Vec<_>>();
    indexes.sort_unstable();
    indexes.truncate(max_segments);
    indexes
}

pub(super) fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}
