use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::durability::WalHighWatermark;
use crate::engine::encoder::Encoder;
use crate::engine::fs_utils::{remove_dir_if_exists, write_file_atomically_and_sync_parent};
use crate::engine::segment::{
    is_not_found_error, list_segment_dirs, load_segments_for_level_runtime_strict,
    load_segments_runtime_strict, read_segment_manifest, segment_validation_error, LoadedSegment,
    PersistedSeries, SegmentValidationContext, SegmentWriter,
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

pub(in crate::engine) use self::execution::finalize_pending_compaction_replacements;

const DEFAULT_L0_TRIGGER: usize = 4;
const DEFAULT_L1_TRIGGER: usize = 4;
const DEFAULT_SOURCE_WINDOW_SEGMENTS: usize = 8;
const DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER: usize = 512;
const COMPACTION_REPLACEMENT_DIR: &str = ".compaction-replacements";
const COMPACTION_REPLACEMENT_VERSION: u16 = 1;
static COMPACTION_REPLACEMENT_COUNTER: AtomicU64 = AtomicU64::new(1);

type SeriesChunkRefs<'a> = HashMap<SeriesId, Vec<&'a Chunk>>;
type MergeSegmentsOutput<'a> = (Vec<PersistedSeries>, SeriesChunkRefs<'a>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactionReplacementMarker {
    version: u16,
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
}

impl Compactor {
    pub fn new(data_path: impl AsRef<Path>, point_cap: usize) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: None,
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
        }
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
