use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::engine::chunk::Chunk;
use crate::engine::fs_utils::{
    remove_path_if_exists, rename_tmp, sync_dir, sync_parent_dir, write_tmp_and_sync,
};
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::{Result, TsinkError};

use super::format::{
    build_chunk_index_file, build_chunks_and_index, build_manifest_file, build_postings_file,
    build_segment_series_data, build_series_file, hash64, ManifestFileEntry, FILE_KIND_CHUNKS,
    FILE_KIND_CHUNK_INDEX, FILE_KIND_POSTINGS, FILE_KIND_SERIES,
};
use super::types::{SegmentLayout, SegmentManifest};
use super::WalHighWatermark;

#[derive(Debug)]
pub struct SegmentWriter {
    pub(super) layout: SegmentLayout,
    pub(super) staging_layout: SegmentLayout,
    segment_id: u64,
    level: u8,
}

impl SegmentWriter {
    pub fn new(base: impl AsRef<Path>, level: u8, segment_id: u64) -> Result<Self> {
        let layout = SegmentLayout::new(base, level, segment_id);
        let staging_root = layout
            .root
            .with_file_name(format!(".tmp-seg-{segment_id:016x}"));
        let staging_layout = SegmentLayout::from_root(staging_root);
        Ok(Self {
            layout,
            staging_layout,
            segment_id,
            level,
        })
    }

    pub fn layout(&self) -> &SegmentLayout {
        &self.layout
    }

    pub fn write_segment<T>(
        &self,
        registry: &SeriesRegistry,
        chunks_by_series: &HashMap<SeriesId, Vec<T>>,
    ) -> Result<SegmentManifest>
    where
        T: AsRef<Chunk>,
    {
        self.write_segment_with_wal_highwater(
            registry,
            chunks_by_series,
            WalHighWatermark::default(),
        )
    }

    pub fn write_segment_with_wal_highwater<T>(
        &self,
        registry: &SeriesRegistry,
        chunks_by_series: &HashMap<SeriesId, Vec<T>>,
        wal_highwater: WalHighWatermark,
    ) -> Result<SegmentManifest>
    where
        T: AsRef<Chunk>,
    {
        prepare_staging_dir(&self.staging_layout.root)?;

        let (chunks_bytes, mut chunk_index, chunk_count, point_count, min_ts, max_ts) =
            build_chunks_and_index(self.level, chunks_by_series)?;
        let series_data = build_segment_series_data(registry, chunks_by_series)?;
        let (series_bytes, series_count) = build_series_file(&series_data)?;
        let postings_bytes = build_postings_file(&series_data)?;
        let chunk_index_bytes = build_chunk_index_file(&mut chunk_index)?;
        let manifest_files = [
            ManifestFileEntry {
                kind: FILE_KIND_CHUNKS,
                file_len: chunks_bytes.len() as u64,
                hash64: hash64(&chunks_bytes),
            },
            ManifestFileEntry {
                kind: FILE_KIND_CHUNK_INDEX,
                file_len: chunk_index_bytes.len() as u64,
                hash64: hash64(&chunk_index_bytes),
            },
            ManifestFileEntry {
                kind: FILE_KIND_SERIES,
                file_len: series_bytes.len() as u64,
                hash64: hash64(&series_bytes),
            },
            ManifestFileEntry {
                kind: FILE_KIND_POSTINGS,
                file_len: postings_bytes.len() as u64,
                hash64: hash64(&postings_bytes),
            },
        ];

        let data_files = [
            (&self.staging_layout.chunks_path, chunks_bytes),
            (&self.staging_layout.chunk_index_path, chunk_index_bytes),
            (&self.staging_layout.series_path, series_bytes),
            (&self.staging_layout.postings_path, postings_bytes),
        ];

        let mut staged_files = Vec::with_capacity(data_files.len());
        for (path, bytes) in &data_files {
            staged_files.push((write_tmp_and_sync(path, bytes)?, *path));
        }

        for (tmp_path, path) in &staged_files {
            rename_tmp(tmp_path, path)?;
        }

        let manifest = SegmentManifest {
            segment_id: self.segment_id,
            level: self.level,
            chunk_count,
            point_count,
            series_count,
            min_ts,
            max_ts,
            wal_highwater,
        };

        let manifest_bytes = build_manifest_file(&manifest, manifest_files)?;
        let manifest_tmp_path =
            write_tmp_and_sync(&self.staging_layout.manifest_path, &manifest_bytes)?;
        rename_tmp(&manifest_tmp_path, &self.staging_layout.manifest_path)?;

        sync_dir(&self.staging_layout.root)?;
        ensure_publish_target_clear(&self.layout)?;
        fs::rename(&self.staging_layout.root, &self.layout.root)?;

        let publish_sync_result = (|| -> Result<()> {
            sync_dir(&self.layout.root)?;
            if let Some(level_root) = self.layout.root.parent() {
                sync_dir(level_root)?;
            }
            Ok(())
        })();
        if let Err(err) = publish_sync_result {
            if let Err(rollback_err) = rollback_failed_segment_publish(&self.layout.root) {
                return Err(TsinkError::Other(format!(
                    "segment publish sync failed and rollback failed: publish={err}, rollback={rollback_err}"
                )));
            }
            return Err(err);
        }

        Ok(manifest)
    }
}

fn prepare_staging_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Err(TsinkError::InvalidConfiguration(
            "staging directory has no parent".to_string(),
        ));
    };
    fs::create_dir_all(parent)?;
    remove_path_if_exists(path)?;
    fs::create_dir_all(path)?;
    Ok(())
}

fn ensure_publish_target_clear(layout: &SegmentLayout) -> Result<()> {
    if !layout.root.exists() {
        return Ok(());
    }

    if !layout.manifest_path.exists() {
        remove_path_if_exists(&layout.root)?;
        return Ok(());
    }

    Err(TsinkError::InvalidConfiguration(format!(
        "segment directory already exists: {}",
        layout.root.display()
    )))
}

fn rollback_failed_segment_publish(root: &Path) -> Result<()> {
    remove_path_if_exists(root)?;
    sync_parent_dir(root)?;
    Ok(())
}
