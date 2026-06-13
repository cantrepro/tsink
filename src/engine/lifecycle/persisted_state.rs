#[path = "persisted_state/install.rs"]
mod install;
#[path = "persisted_state/postings.rs"]
mod postings;
#[path = "persisted_state/validation.rs"]
mod validation;
#[path = "persisted_state/visibility.rs"]
mod visibility;

use std::collections::{BTreeMap, BTreeSet};

use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
use super::{
    saturating_u64_from_usize, state, Arc, ChunkStorage, HashMap, Label,
    LifecyclePublicationContext, Ordering, Path, PathBuf, PersistedChunkRef, PersistedIndexState,
    Result, SeriesId, SeriesRegistry, SeriesValueFamily, TsinkError,
};
use crate::engine::segment::{
    IndexedSegment, LoadedSegmentIndexes, PersistedSeries, SegmentPostingsIndex,
};

struct PreparedLoadedPersistedSegments {
    indexed_segments: Vec<IndexedSegment>,
    series: Vec<PersistedSeries>,
}

#[derive(Default)]
struct PersistedSegmentsStateUpdate {
    affected_series: Vec<SeriesId>,
    loaded_max_timestamp: Option<i64>,
}

struct PersistedSegmentRemovalStateUpdate {
    removed_any: bool,
    affected_series: Vec<SeriesId>,
}

struct PreparedPersistedIndexInstall {
    persisted_index: PersistedIndexState,
    persisted_series_ids: Vec<SeriesId>,
    loaded_max_timestamp: Option<i64>,
    next_segment_id: u64,
}

struct PersistedVisibleStateInstall {
    refresh_series_ids: Vec<SeriesId>,
    mark_refreshed_series_materialized: bool,
    loaded_max_timestamp: Option<i64>,
    next_segment_id: Option<u64>,
}

impl ChunkStorage {
    pub(in super::super) fn add_persisted_segments_from_loaded(
        &self,
        loaded_segments: Vec<IndexedSegment>,
    ) -> Result<()> {
        let Some(PreparedLoadedPersistedSegments {
            indexed_segments,
            series,
        }) = Self::prepare_loaded_persisted_segments(loaded_segments)?
        else {
            return Ok(());
        };

        self.register_persisted_series_and_value_families(&series, &indexed_segments)?;
        let state_update = self.apply_loaded_segments_state_update(indexed_segments, &series)?;
        self.install_visible_persisted_state(PersistedVisibleStateInstall {
            refresh_series_ids: state_update.affected_series,
            mark_refreshed_series_materialized: true,
            loaded_max_timestamp: state_update.loaded_max_timestamp,
            next_segment_id: None,
        })
    }

    pub(in super::super) fn remove_persisted_segment_roots(
        &self,
        roots: &[PathBuf],
    ) -> Result<bool> {
        if roots.is_empty() {
            return Ok(false);
        }

        let state_update = self.apply_segment_root_removal_state_update(roots)?;
        self.install_visible_persisted_state(PersistedVisibleStateInstall {
            refresh_series_ids: state_update.affected_series,
            mark_refreshed_series_materialized: false,
            loaded_max_timestamp: None,
            next_segment_id: None,
        })?;
        Ok(state_update.removed_any)
    }

    pub(in super::super) fn apply_loaded_segment_indexes(
        &self,
        loaded: LoadedSegmentIndexes,
        reconcile_registry_with_persisted: bool,
    ) -> Result<()> {
        Self::validate_loaded_segment_indexes(&loaded, reconcile_registry_with_persisted)?;
        if !loaded.series.is_empty() {
            self.register_persisted_series_and_value_families(
                &loaded.series,
                &loaded.indexed_segments,
            )?;
        }

        let mut prepared_install = self.prepare_persisted_index_install(loaded)?;
        if reconcile_registry_with_persisted {
            self.reconcile_registry_with_persisted_series_ids(
                &prepared_install.persisted_series_ids,
            );
        }

        self.rebuild_merged_postings_from_registry(&mut prepared_install.persisted_index)?;
        self.install_persisted_index_rebuild(prepared_install)
    }

    pub(in super::super) fn persisted_segment_location_for_root(
        &self,
        root: &Path,
    ) -> Result<(SegmentLaneFamily, PersistedSegmentTier)> {
        for (lane, hot_root) in [
            (
                SegmentLaneFamily::Numeric,
                self.persisted.numeric_lane_path.as_deref(),
            ),
            (
                SegmentLaneFamily::Blob,
                self.persisted.blob_lane_path.as_deref(),
            ),
        ] {
            if hot_root.is_some_and(|hot_root| root.starts_with(hot_root)) {
                return Ok((lane, PersistedSegmentTier::Hot));
            }
        }

        if let Some(tiered_storage) = &self.persisted.tiered_storage {
            for tier in [
                PersistedSegmentTier::Hot,
                PersistedSegmentTier::Warm,
                PersistedSegmentTier::Cold,
            ] {
                for lane in [SegmentLaneFamily::Numeric, SegmentLaneFamily::Blob] {
                    let lane_path = tiered_storage.lane_path(lane, tier);
                    if root.starts_with(&lane_path) {
                        return Ok((lane, tier));
                    }
                }
            }
        }

        Err(TsinkError::InvalidConfiguration(format!(
            "persisted segment root is outside configured storage lanes: {}",
            root.display()
        )))
    }

    pub(in super::super) fn rollback_published_segment_roots(
        &self,
        segment_roots: &[PathBuf],
    ) -> Result<()> {
        for root in segment_roots.iter().rev() {
            crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(root)?;
        }

        Ok(())
    }
}
