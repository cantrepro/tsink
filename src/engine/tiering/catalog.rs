use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::inventory::SegmentInventoryAccumulator;
use super::layout::{relative_segment_path, SegmentPathResolver};
use super::{PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily};
use crate::engine::fs_utils::write_file_atomically_and_sync_parent;
use crate::engine::segment::{SegmentManifest, WalHighWatermark};

pub(in crate::engine::storage_engine) const SEGMENT_CATALOG_FILE_NAME: &str =
    "segment_catalog.json";
const SEGMENT_CATALOG_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentCatalogFile {
    version: u32,
    entries: Vec<SegmentCatalogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentCatalogEntry {
    lane: SegmentLaneFamily,
    tier: PersistedSegmentTier,
    level: u8,
    segment_id: u64,
    #[serde(default)]
    chunk_count: usize,
    #[serde(default)]
    point_count: usize,
    #[serde(default)]
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    #[serde(default)]
    wal_highwater_segment: u64,
    #[serde(default)]
    wal_highwater_frame: u64,
    relative_path: String,
}

pub(in crate::engine::storage_engine) fn persist_segment_catalog(
    path: &Path,
    inventory: &SegmentInventory,
) -> Result<()> {
    let entries = inventory
        .entries()
        .iter()
        .map(|entry| SegmentCatalogEntry {
            lane: entry.lane,
            tier: entry.tier,
            level: entry.manifest.level,
            segment_id: entry.manifest.segment_id,
            chunk_count: entry.manifest.chunk_count,
            point_count: entry.manifest.point_count,
            series_count: entry.manifest.series_count,
            min_ts: entry.manifest.min_ts,
            max_ts: entry.manifest.max_ts,
            wal_highwater_segment: entry.manifest.wal_highwater.segment,
            wal_highwater_frame: entry.manifest.wal_highwater.frame,
            relative_path: relative_segment_path(&entry.manifest)
                .to_string_lossy()
                .into_owned(),
        })
        .collect::<Vec<_>>();

    let bytes = serde_json::to_vec_pretty(&SegmentCatalogFile {
        version: SEGMENT_CATALOG_VERSION,
        entries,
    })?;
    write_file_atomically_and_sync_parent(path, &bytes)
}

pub(in crate::engine::storage_engine) fn shared_segment_catalog_path(
    config: &TieredStorageConfig,
) -> PathBuf {
    config.object_store_root.join(SEGMENT_CATALOG_FILE_NAME)
}

pub(in crate::engine::storage_engine) fn load_segment_catalog(
    path: &Path,
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<SegmentInventory> {
    let bytes = std::fs::read(path)?;
    let file: SegmentCatalogFile = serde_json::from_slice(&bytes)?;
    if !(1..=SEGMENT_CATALOG_VERSION).contains(&file.version) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "unsupported segment catalog version {} at {}",
            file.version,
            path.display()
        )));
    }

    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();
    for entry in file.entries {
        deduped.insert(catalog_entry_to_inventory_entry(entry, resolver)?);
    }

    Ok(deduped.finish())
}

fn catalog_entry_to_inventory_entry(
    entry: SegmentCatalogEntry,
    resolver: SegmentPathResolver<'_>,
) -> Result<SegmentInventoryEntry> {
    let manifest = SegmentManifest {
        segment_id: entry.segment_id,
        level: entry.level,
        chunk_count: entry.chunk_count,
        point_count: entry.point_count,
        series_count: entry.series_count,
        min_ts: entry.min_ts,
        max_ts: entry.max_ts,
        wal_highwater: WalHighWatermark {
            segment: entry.wal_highwater_segment,
            frame: entry.wal_highwater_frame,
        },
    };
    let relative_path = validated_catalog_relative_path(&entry.relative_path, &manifest)?;
    let base_path = resolver.catalog_lane_root(entry.lane, entry.tier)?;
    Ok(SegmentInventoryEntry {
        lane: entry.lane,
        tier: entry.tier,
        root: base_path.join(relative_path),
        manifest,
    })
}

fn validated_catalog_relative_path(
    relative_path: &str,
    manifest: &SegmentManifest,
) -> Result<PathBuf> {
    let path = PathBuf::from(relative_path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::ParentDir
            )
        })
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "segment catalog entry path must stay within the configured lane root: {relative_path}"
        )));
    }

    let expected = relative_segment_path(manifest);
    if path != expected {
        return Err(TsinkError::InvalidConfiguration(format!(
            "segment catalog entry path {relative_path} did not match expected relative path {}",
            expected.display()
        )));
    }

    Ok(path)
}
