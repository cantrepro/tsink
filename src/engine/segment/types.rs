use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::engine::chunk::Chunk;
use crate::engine::durability::WalHighWatermark;
use crate::engine::index::ChunkIndex;
use crate::engine::series::{SeriesId, SeriesValueFamily};
use crate::mmap::PlatformMmap;
use crate::Label;

use super::postings::SegmentPostingsIndex;

#[derive(Debug, Clone)]
pub struct SegmentLayout {
    pub root: PathBuf,
    pub chunks_path: PathBuf,
    pub chunk_index_path: PathBuf,
    pub series_path: PathBuf,
    pub postings_path: PathBuf,
    pub manifest_path: PathBuf,
}

impl SegmentLayout {
    pub fn new(base: impl AsRef<Path>, level: u8, segment_id: u64) -> Self {
        let root = base
            .as_ref()
            .join("segments")
            .join(format!("L{level}"))
            .join(format!("seg-{segment_id:016x}"));
        Self::from_root(root)
    }

    pub(super) fn from_root(root: PathBuf) -> Self {
        Self {
            chunks_path: root.join("chunks.bin"),
            chunk_index_path: root.join("chunk_index.bin"),
            series_path: root.join("series.bin"),
            postings_path: root.join("postings.bin"),
            manifest_path: root.join("manifest.bin"),
            root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifest {
    pub segment_id: u64,
    pub level: u8,
    pub chunk_count: usize,
    pub point_count: usize,
    pub series_count: usize,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub wal_highwater: WalHighWatermark,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentContentFingerprint {
    pub(crate) manifest: SegmentManifest,
    pub(crate) files: [SegmentFileFingerprint; super::format::MANIFEST_FILE_ENTRY_COUNT],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentFileFingerprint {
    pub(crate) kind: u8,
    pub(crate) file_len: u64,
    pub(crate) hash64: u64,
}

#[derive(Debug, Clone)]
pub struct PersistedSeries {
    pub series_id: SeriesId,
    pub metric: String,
    pub labels: Vec<Label>,
    pub value_family: Option<SeriesValueFamily>,
}

#[derive(Debug, Default)]
pub struct LoadedSegments {
    pub next_segment_id: u64,
    pub series: Vec<PersistedSeries>,
    pub chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

#[derive(Debug, Clone)]
pub struct LoadedSegment {
    pub root: PathBuf,
    pub manifest: SegmentManifest,
    pub series: Vec<PersistedSeries>,
    pub chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

pub struct IndexedSegment {
    pub root: PathBuf,
    pub manifest: SegmentManifest,
    pub series: Vec<PersistedSeries>,
    pub(crate) postings: SegmentPostingsIndex,
    pub chunk_index: ChunkIndex,
    pub chunks_mmap: PlatformMmap,
}

#[derive(Default)]
pub struct LoadedSegmentIndexes {
    pub next_segment_id: u64,
    pub series: Vec<PersistedSeries>,
    pub indexed_segments: Vec<IndexedSegment>,
    pub wal_replay_highwater: WalHighWatermark,
}
