use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::durability::WalHighWatermark;
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    is_not_found_error, load_segments_runtime_strict, segment_validation_error, LoadedSegment,
    PersistedSeries, SegmentManifest, SegmentValidationContext, SegmentWriter,
};
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::engine::tombstone::{
    load_tombstones, timestamp_is_tombstoned, TombstoneMap, TombstoneRange, TOMBSTONES_FILE_NAME,
};
use crate::{Result, TsinkError};
use serde::{Deserialize, Serialize};

#[path = "compactor/execution.rs"]
mod execution;
#[path = "compactor/planning.rs"]
mod planning;

#[cfg(test)]
pub(in crate::engine) use self::execution::finalize_pending_compaction_replacements;
pub(in crate::engine) use self::execution::finalize_pending_compaction_replacements_with_disk_budget;

const DEFAULT_L0_TRIGGER: usize = 4;
const DEFAULT_L1_TRIGGER: usize = 4;
const DEFAULT_SOURCE_WINDOW_SEGMENTS: usize = 8;
const DEFAULT_MAINTENANCE_MAX_ITEMS_PER_PASS: usize = 1_024;
const DEFAULT_MAINTENANCE_MAX_BYTES_PER_PASS: u64 = 256 * 1024 * 1024;
const DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER: usize = 512;
const COMPACTION_REPLACEMENT_DIR: &str = ".compaction-replacements";
const LEGACY_COMPACTION_REPLACEMENT_VERSION: u16 = 1;
const COMPACTION_REPLACEMENT_VERSION: u16 = 2;
static COMPACTION_REPLACEMENT_COUNTER: AtomicU64 = AtomicU64::new(1);

type SeriesChunkRefs<'a> = HashMap<SeriesId, Vec<&'a Chunk>>;
type MergeSegmentsOutput<'a> = (Vec<PersistedSeries>, SeriesChunkRefs<'a>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CompactionReplacementPhase {
    Preparing,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactionReplacementMarker {
    version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    phase: Option<CompactionReplacementPhase>,
    source_segments: Vec<String>,
    output_segments: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionRunStats {
    pub compacted: bool,
    pub source_level: Option<u8>,
    pub target_level: Option<u8>,
    pub source_segments: usize,
    pub output_segments: usize,
    pub source_chunks: usize,
    pub output_chunks: usize,
    pub source_points: usize,
    pub output_points: usize,
    /// Directory entries inspected by the bounded planner in this pass.
    pub planning_directory_entries_inspected: usize,
    /// Segment manifests inspected by the bounded planner in this pass.
    pub planning_manifests_inspected: usize,
    /// Candidate segment manifests retained at the end of planning.
    pub planning_candidates_observed: usize,
    /// Modeled source bytes admitted before any selected segment was fully loaded.
    pub planning_source_bytes: u64,
    /// Whether more namespace or candidate work was observed than this pass admitted.
    pub planning_backlog_observed: bool,
    /// Whether any directory, manifest, source-item, or byte budget stopped this pass.
    pub planning_budget_exhausted: bool,
}

/// Finite work limits applied by one standalone compactor pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPassLimits {
    pub max_directory_entries: usize,
    pub max_manifest_inspections: usize,
    pub max_source_segments: usize,
    pub max_source_chunks: usize,
    pub max_source_points: usize,
    pub max_decoded_bytes: u64,
}

impl Default for CompactionPassLimits {
    fn default() -> Self {
        maintenance_compaction_pass_limits(
            DEFAULT_MAINTENANCE_MAX_ITEMS_PER_PASS,
            DEFAULT_MAINTENANCE_MAX_BYTES_PER_PASS,
        )
    }
}

fn maintenance_compaction_pass_limits(
    max_items_per_pass: usize,
    max_bytes_per_pass: u64,
) -> CompactionPassLimits {
    let modeled_point_bytes = std::mem::size_of::<ChunkPoint>().max(1) as u64;
    CompactionPassLimits {
        max_directory_entries: max_items_per_pass,
        max_manifest_inspections: max_items_per_pass,
        max_source_segments: max_items_per_pass.min(DEFAULT_SOURCE_WINDOW_SEGMENTS),
        max_source_chunks: max_items_per_pass,
        max_source_points: usize::try_from(max_bytes_per_pass / modeled_point_bytes)
            .unwrap_or(usize::MAX),
        max_decoded_bytes: max_bytes_per_pass,
    }
}

#[derive(Debug, Clone)]
struct SegmentCandidate {
    root: PathBuf,
    manifest: SegmentManifest,
    persisted_file_bytes: u64,
    persisted_chunks_file_bytes: u64,
}

#[derive(Debug, Default)]
struct LevelPlanningCursor {
    entries: Option<fs::ReadDir>,
    candidates: VecDeque<SegmentCandidate>,
}

#[derive(Debug, Default)]
struct CompactionPlanningState {
    levels: [LevelPlanningCursor; 2],
    next_level: usize,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub(super) struct CompactionOutcome {
    pub(super) stats: CompactionRunStats,
    pub(super) source_roots: Vec<PathBuf>,
    pub(super) output_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionLevel {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone)]
pub struct Compactor {
    data_path: PathBuf,
    point_cap: usize,
    l0_trigger: usize,
    l1_trigger: usize,
    next_segment_id: Option<Arc<AtomicU64>>,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    output_disk_category: crate::DiskCategory,
    pass_limits: CompactionPassLimits,
    planning_state: Arc<Mutex<CompactionPlanningState>>,
}

impl Compactor {
    pub fn new(data_path: impl AsRef<Path>, point_cap: usize) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: None,
            local_disk_budget: None,
            output_disk_category: crate::DiskCategory::Segments,
            pass_limits: CompactionPassLimits::default(),
            planning_state: Arc::new(Mutex::new(CompactionPlanningState::default())),
        }
    }

    pub fn new_with_segment_id_allocator(
        data_path: impl AsRef<Path>,
        point_cap: usize,
        next_segment_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: Some(next_segment_id),
            local_disk_budget: None,
            output_disk_category: crate::DiskCategory::Segments,
            pass_limits: CompactionPassLimits::default(),
            planning_state: Arc::new(Mutex::new(CompactionPlanningState::default())),
        }
    }

    pub(in crate::engine) fn new_with_segment_id_allocator_and_disk_budget(
        data_path: impl AsRef<Path>,
        point_cap: usize,
        next_segment_id: Arc<AtomicU64>,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: Some(next_segment_id),
            local_disk_budget,
            output_disk_category: crate::DiskCategory::Segments,
            pass_limits: CompactionPassLimits::default(),
            planning_state: Arc::new(Mutex::new(CompactionPlanningState::default())),
        }
    }

    pub(in crate::engine) fn with_output_disk_category(
        mut self,
        category: crate::DiskCategory,
    ) -> Self {
        self.output_disk_category = category;
        self
    }

    /// Applies the shared maintenance item/byte ceilings to compaction planning and source load.
    pub fn with_maintenance_work_limits(
        mut self,
        max_items_per_pass: usize,
        max_bytes_per_pass: u64,
    ) -> Self {
        self.pass_limits =
            maintenance_compaction_pass_limits(max_items_per_pass, max_bytes_per_pass);
        self
    }

    /// Applies detailed standalone compaction limits. Values are hard ceilings; zero yields.
    pub fn with_compaction_pass_limits(mut self, limits: CompactionPassLimits) -> Self {
        self.pass_limits = limits;
        self
    }

    pub fn compact_once(&self) -> Result<bool> {
        Ok(self.compact_once_with_stats()?.compacted)
    }

    pub fn compact_once_with_stats(&self) -> Result<CompactionRunStats> {
        Ok(self.compact_once_with_changes()?.stats)
    }
}

#[cfg(test)]
#[path = "compactor/tests.rs"]
mod tests;
