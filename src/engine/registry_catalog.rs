use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::Xxh64;

use super::tiering::SegmentLaneFamily;
use super::*;
use crate::engine::fs_utils::{path_exists_no_follow, write_file_atomically_and_sync_parent};
use crate::engine::segment::IndexedSegment;
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::Label;

const REGISTRY_CATALOG_FILE_NAME: &str = "series_index.catalog.json";
const REGISTRY_CATALOG_VERSION: u32 = 2;

#[derive(Debug, Clone)]
pub(super) struct PersistedRegistryCatalogSource {
    pub(super) lane: SegmentLaneFamily,
    pub(super) root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogFile {
    version: u32,
    segments: Vec<PersistedRegistryCatalogEntry>,
    #[serde(default)]
    series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogEntry {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
    chunk_count: usize,
    point_count: usize,
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    wal_highwater_segment: u64,
    wal_highwater_frame: u64,
    chunks_len: u64,
    chunks_hash64: u64,
    chunk_index_len: u64,
    chunk_index_hash64: u64,
    series_len: u64,
    series_hash64: u64,
    postings_len: u64,
    postings_hash64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedRegistrySeriesFingerprint {
    series_count: usize,
    series_hash64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedRegistryCatalog {
    pub(super) series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

pub(super) fn catalog_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_FILE_NAME))
}

pub(super) fn validate_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Option<ValidatedRegistryCatalog>> {
    let path = catalog_path(snapshot_path);
    if !path_exists_no_follow(&path)? {
        return Ok(None);
    }

    let expected_segments = build_catalog_entries(sources)?;
    let actual = serde_json::from_slice::<PersistedRegistryCatalogFile>(&std::fs::read(&path)?)?;
    if actual.segments != expected_segments {
        return Ok(None);
    }

    Ok(Some(ValidatedRegistryCatalog {
        series_fingerprint: actual.series_fingerprint,
    }))
}

pub(super) fn persist_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
) -> Result<()> {
    let path = catalog_path(snapshot_path);
    let bytes = serde_json::to_vec_pretty(&build_catalog(sources)?)?;
    write_file_atomically_and_sync_parent(&path, &bytes)
}

pub(super) fn inventory_sources(
    inventory: &super::tiering::SegmentInventory,
) -> Vec<PersistedRegistryCatalogSource> {
    inventory
        .entries()
        .iter()
        .map(|entry| PersistedRegistryCatalogSource {
            lane: entry.lane,
            root: entry.root.clone(),
        })
        .collect()
}

pub(super) fn validate_registry_snapshot(
    registry: &SeriesRegistry,
    indexed_segments: &[IndexedSegment],
    expected: &PersistedRegistrySeriesFingerprint,
) -> Result<()> {
    let series_ids = indexed_segment_series_ids(indexed_segments);
    let actual = fingerprint_registry_series(registry, &series_ids)?;
    if actual == *expected {
        return Ok(());
    }

    Err(TsinkError::DataCorruption(
        "persisted registry snapshot conflicts with persisted segment series metadata".to_string(),
    ))
}

fn build_catalog(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<PersistedRegistryCatalogFile> {
    Ok(PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: build_catalog_entries(sources)?,
        series_fingerprint: Some(build_series_fingerprint_from_sources(sources)?),
    })
}

fn build_catalog_entries(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Vec<PersistedRegistryCatalogEntry>> {
    let mut segments = sources
        .iter()
        .map(build_catalog_entry)
        .collect::<Result<Vec<_>>>()?;
    segments.sort_by_key(|entry| (entry.lane, entry.level, entry.segment_id));
    Ok(segments)
}

fn build_catalog_entry(
    source: &PersistedRegistryCatalogSource,
) -> Result<PersistedRegistryCatalogEntry> {
    let fingerprint = crate::engine::segment::read_segment_manifest_fingerprint(&source.root)?;
    let [chunks, chunk_index, series, postings] = fingerprint.files;
    Ok(PersistedRegistryCatalogEntry {
        lane: source.lane,
        level: fingerprint.manifest.level,
        segment_id: fingerprint.manifest.segment_id,
        chunk_count: fingerprint.manifest.chunk_count,
        point_count: fingerprint.manifest.point_count,
        series_count: fingerprint.manifest.series_count,
        min_ts: fingerprint.manifest.min_ts,
        max_ts: fingerprint.manifest.max_ts,
        wal_highwater_segment: fingerprint.manifest.wal_highwater.segment,
        wal_highwater_frame: fingerprint.manifest.wal_highwater.frame,
        chunks_len: chunks.file_len,
        chunks_hash64: chunks.hash64,
        chunk_index_len: chunk_index.file_len,
        chunk_index_hash64: chunk_index.hash64,
        series_len: series.file_len,
        series_hash64: series.hash64,
        postings_len: postings.file_len,
        postings_hash64: postings.hash64,
    })
}

fn build_series_fingerprint_from_sources(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series_by_id = BTreeMap::<SeriesId, crate::engine::segment::PersistedSeries>::new();
    let mut series_id_by_key = BTreeMap::<(String, Vec<Label>), SeriesId>::new();

    for source in sources {
        for series in crate::engine::segment::load_segment_series_metadata(&source.root)? {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels => {}
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts across persisted segment metadata",
                        series.series_id
                    )));
                }
                None => {
                    let key = (series.metric.clone(), series.labels.clone());
                    if let Some(existing_id) = series_id_by_key.get(&key) {
                        if *existing_id != series.series_id {
                            return Err(TsinkError::DataCorruption(format!(
                                "series key already bound to id {}, persisted segment metadata tried to bind {}",
                                existing_id, series.series_id
                            )));
                        }
                    } else {
                        series_id_by_key.insert(key, series.series_id);
                    }
                    series_by_id.insert(series.series_id, series);
                }
            }
        }
    }

    Ok(fingerprint_series(series_by_id.values()))
}

fn indexed_segment_series_ids(indexed_segments: &[IndexedSegment]) -> Vec<SeriesId> {
    let mut series_ids = BTreeSet::new();
    for segment in indexed_segments {
        for entry in &segment.chunk_index.entries {
            series_ids.insert(entry.series_id);
        }
    }
    series_ids.into_iter().collect()
}

fn fingerprint_registry_series(
    registry: &SeriesRegistry,
    series_ids: &[SeriesId],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series = Vec::with_capacity(series_ids.len());
    for series_id in series_ids {
        let Some(series_key) = registry.decode_series_key(*series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "persisted registry snapshot is missing series id {} required by persisted segments",
                series_id
            )));
        };
        series.push(crate::engine::segment::PersistedSeries {
            series_id: *series_id,
            metric: series_key.metric,
            labels: series_key.labels,
            value_family: None,
        });
    }
    Ok(fingerprint_series(series.iter()))
}

fn fingerprint_series<'a>(
    series: impl IntoIterator<Item = &'a crate::engine::segment::PersistedSeries>,
) -> PersistedRegistrySeriesFingerprint {
    let mut count = 0usize;
    let mut hasher = Xxh64::new(0);
    for series in series {
        count = count.saturating_add(1);
        hasher.update(&series.series_id.to_le_bytes());
        update_len_prefixed_bytes(&mut hasher, series.metric.as_bytes());
        hasher.update(&(series.labels.len() as u64).to_le_bytes());
        for label in &series.labels {
            update_len_prefixed_bytes(&mut hasher, label.name.as_bytes());
            update_len_prefixed_bytes(&mut hasher, label.value.as_bytes());
        }
    }
    PersistedRegistrySeriesFingerprint {
        series_count: count,
        series_hash64: hasher.digest(),
    }
}

fn update_len_prefixed_bytes(hasher: &mut Xxh64, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}
