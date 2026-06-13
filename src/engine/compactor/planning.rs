use super::*;

impl Compactor {
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        finalize_pending_compaction_replacements(&self.data_path)?;
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L0,
            CompactionLevel::L1,
            self.l0_trigger,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L1,
            CompactionLevel::L2,
            self.l1_trigger,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        Ok(CompactionOutcome::default())
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        if count_level_segments_for_compaction(&self.data_path, source_level)? < 2 {
            return Ok(None);
        }

        let mut segments = load_segments_for_level_runtime_strict(&self.data_path, source_level)?;
        if segments.len() < 2 {
            return Ok(None);
        }

        segments.sort_by_key(|segment| segment.manifest.segment_id);

        let should_compact = segments.len() >= count_trigger || has_time_overlap(&segments);
        if !should_compact {
            return Ok(None);
        }

        let window =
            select_compaction_window(&segments, count_trigger, DEFAULT_SOURCE_WINDOW_SEGMENTS);
        if window.len() < 2 {
            return Ok(None);
        }

        let mut outcome = self.compact_segments(target_level, &window, tombstones)?;
        outcome.stats.compacted = true;
        outcome.stats.source_level = Some(source_level);
        outcome.stats.target_level = Some(target_level);
        Ok(Some(outcome))
    }
}

pub(super) fn count_level_segments_for_compaction(base: &Path, level: u8) -> Result<usize> {
    let expected_level_dir = format!("L{level}");
    let mut count = 0usize;

    for root in list_segment_dirs(base)? {
        if root
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(expected_level_dir.as_str())
        {
            continue;
        }

        match read_segment_manifest(&root) {
            Ok(manifest) => {
                if manifest.level != level {
                    return Err(segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        &format!(
                            "segment directory level mismatch: stored under L{level}, manifest says L{}",
                            manifest.level
                        ),
                    ));
                }
                count = count.saturating_add(1);
            }
            Err(err) if is_not_found_error(&err) => continue,
            Err(TsinkError::DataCorruption(details)) => {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &details,
                ));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(count)
}

pub(super) fn has_time_overlap(segments: &[LoadedSegment]) -> bool {
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

pub(super) fn select_compaction_window(
    segments: &[LoadedSegment],
    count_trigger: usize,
    max_segments: usize,
) -> Vec<&LoadedSegment> {
    let max_segments = max_segments.max(2);
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
    segments: &[LoadedSegment],
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
    indexes.truncate(max_segments.max(2));
    indexes
}

pub(super) fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}
