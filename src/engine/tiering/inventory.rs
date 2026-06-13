use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::super::{BLOB_LANE_ROOT, NUMERIC_LANE_ROOT};
use crate::engine::segment::SegmentManifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::engine::storage_engine) enum SegmentLaneFamily {
    Numeric,
    Blob,
}

impl SegmentLaneFamily {
    pub(in crate::engine::storage_engine) fn root_name(self) -> &'static str {
        match self {
            Self::Numeric => NUMERIC_LANE_ROOT,
            Self::Blob => BLOB_LANE_ROOT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::engine::storage_engine) enum PersistedSegmentTier {
    Hot,
    Warm,
    Cold,
}

impl PersistedSegmentTier {
    fn priority(self) -> u8 {
        match self {
            Self::Hot => 0,
            Self::Warm => 1,
            Self::Cold => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct SegmentInventoryEntry {
    pub(in crate::engine::storage_engine) lane: SegmentLaneFamily,
    pub(in crate::engine::storage_engine) tier: PersistedSegmentTier,
    pub(in crate::engine::storage_engine) root: PathBuf,
    pub(in crate::engine::storage_engine) manifest: SegmentManifest,
}

#[derive(Debug, Default, Clone)]
pub(in crate::engine::storage_engine) struct SegmentInventory {
    entries: Vec<SegmentInventoryEntry>,
}

impl SegmentInventory {
    pub(in crate::engine::storage_engine) fn from_entries(
        mut entries: Vec<SegmentInventoryEntry>,
    ) -> Self {
        entries.sort_by(|a, b| {
            (
                a.lane,
                a.tier,
                a.manifest.level,
                a.manifest.segment_id,
                &a.root,
            )
                .cmp(&(
                    b.lane,
                    b.tier,
                    b.manifest.level,
                    b.manifest.segment_id,
                    &b.root,
                ))
        });
        Self { entries }
    }

    pub(in crate::engine::storage_engine) fn entries(&self) -> &[SegmentInventoryEntry] {
        &self.entries
    }

    pub(in crate::engine::storage_engine) fn root_set(&self) -> BTreeSet<PathBuf> {
        self.entries
            .iter()
            .map(|entry| entry.root.clone())
            .collect()
    }

    pub(in crate::engine::storage_engine) fn roots_for_lane(
        &self,
        lane: SegmentLaneFamily,
    ) -> Vec<PathBuf> {
        let mut roots = self
            .entries
            .iter()
            .filter(|entry| entry.lane == lane)
            .map(|entry| entry.root.clone())
            .collect::<Vec<_>>();
        roots.sort_by_key(|root| root.to_string_lossy().into_owned());
        roots
    }

    pub(in crate::engine::storage_engine) fn tier_counts(&self) -> (u64, u64, u64) {
        let mut hot = 0u64;
        let mut warm = 0u64;
        let mut cold = 0u64;
        for entry in &self.entries {
            match entry.tier {
                PersistedSegmentTier::Hot => hot = hot.saturating_add(1),
                PersistedSegmentTier::Warm => warm = warm.saturating_add(1),
                PersistedSegmentTier::Cold => cold = cold.saturating_add(1),
            }
        }
        (hot, warm, cold)
    }

    pub(in crate::engine::storage_engine) fn without_roots(
        &self,
        excluded_roots: &BTreeSet<PathBuf>,
    ) -> Self {
        Self::from_entries(
            self.entries
                .iter()
                .filter(|entry| !excluded_roots.contains(&entry.root))
                .cloned()
                .collect(),
        )
    }
}

#[derive(Debug, Default)]
pub(super) struct SegmentInventoryAccumulator {
    entries: BTreeMap<SegmentInventoryKey, SegmentInventoryEntry>,
}

impl SegmentInventoryAccumulator {
    pub(super) fn insert(&mut self, candidate: SegmentInventoryEntry) {
        let key = SegmentInventoryKey {
            lane: candidate.lane,
            level: candidate.manifest.level,
            segment_id: candidate.manifest.segment_id,
        };

        match self.entries.get(&key) {
            Some(current) if !prefer_candidate(&candidate, current) => {}
            _ => {
                self.entries.insert(key, candidate);
            }
        }
    }

    pub(super) fn finish(self) -> SegmentInventory {
        SegmentInventory::from_entries(self.entries.into_values().collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentInventoryKey {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
}

fn prefer_candidate(candidate: &SegmentInventoryEntry, current: &SegmentInventoryEntry) -> bool {
    (
        candidate.tier.priority(),
        candidate.root.to_string_lossy().into_owned(),
    ) > (
        current.tier.priority(),
        current.root.to_string_lossy().into_owned(),
    )
}
