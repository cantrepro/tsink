use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::RwLock;

use super::value_family::{
    decode_optional_series_value_family, encode_optional_series_value_family,
};
use super::*;
use crate::engine::binio::{
    append_u16, append_u32, append_u64, decode_optional_zstd_framed_file,
    encode_optional_zstd_framed_file, read_array, read_bytes, read_u16, read_u32, read_u64,
};
use crate::engine::fs_utils::{
    path_exists_no_follow, sync_parent_dir, write_file_atomically_and_sync_parent,
};
use crate::{Result, TsinkError};

impl SeriesRegistry {
    pub fn incremental_path(snapshot_path: &Path) -> PathBuf {
        snapshot_path
            .parent()
            .map(|parent| parent.join(REGISTRY_INCREMENTAL_FILE_NAME))
            .unwrap_or_else(|| PathBuf::from(REGISTRY_INCREMENTAL_FILE_NAME))
    }

    pub fn incremental_dir(snapshot_path: &Path) -> PathBuf {
        snapshot_path
            .parent()
            .map(|parent| parent.join(REGISTRY_INCREMENTAL_DIR_NAME))
            .unwrap_or_else(|| PathBuf::from(REGISTRY_INCREMENTAL_DIR_NAME))
    }

    pub fn load_incremental_state(snapshot_path: &Path) -> Result<Option<LoadedSeriesRegistry>> {
        let legacy_delta_path = Self::incremental_path(snapshot_path);
        let legacy_delta = Self::load_optional_from_path(&legacy_delta_path)?.map(|registry| {
            LoadedSeriesRegistry {
                delta_series_count: registry.series_count(),
                registry,
            }
        });
        let segmented_delta = Self::load_incremental_segments(snapshot_path)?;

        let mut loaded = legacy_delta;
        if let Some(segmented_delta) = segmented_delta {
            Self::merge_loaded_registry(&mut loaded, segmented_delta)?;
        }

        Ok(loaded)
    }

    pub fn load_persisted_state(snapshot_path: &Path) -> Result<Option<LoadedSeriesRegistry>> {
        let snapshot = Self::load_optional_from_path(snapshot_path)?;
        let incremental = Self::load_incremental_state(snapshot_path)?;

        let mut loaded = snapshot.map(|registry| LoadedSeriesRegistry {
            registry,
            delta_series_count: 0,
        });
        if let Some(incremental) = incremental {
            Self::merge_loaded_registry(&mut loaded, incremental)?;
        }

        Ok(loaded)
    }

    pub(crate) fn persist_incremental_to_snapshot_path(&self, snapshot_path: &Path) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let dir_path = Self::incremental_dir(snapshot_path);
        let created_dir = match fs::symlink_metadata(&dir_path) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(TsinkError::DataCorruption(format!(
                        "incremental registry path is not a directory: {}",
                        dir_path.display()
                    )));
                }
                false
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&dir_path)?;
                true
            }
            Err(err) => return Err(err.into()),
        };
        if created_dir {
            sync_parent_dir(&dir_path)?;
        }

        let segment_path = Self::allocate_incremental_segment_path(&dir_path)?;
        self.persist_to_path(&segment_path)
    }

    pub fn persist_to_path(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let metric_dict = self.metric_dict.read();
        let label_name_dict = self.label_name_dict.read();
        let label_value_dict = self.label_value_dict.read();
        let mut series = self.series_definitions_with_families();
        series.sort_by_key(|(entry, _)| entry.series_id);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&REGISTRY_INDEX_MAGIC);
        append_u16(&mut bytes, REGISTRY_INDEX_VERSION);
        append_u16(&mut bytes, 0u16);
        append_u64(&mut bytes, self.next_series_id_value());
        append_u32(&mut bytes, metric_dict.len() as u32);
        append_u32(&mut bytes, label_name_dict.len() as u32);
        append_u32(&mut bytes, label_value_dict.len() as u32);
        append_u64(&mut bytes, series.len() as u64);
        append_u64(&mut bytes, REGISTRY_SECTION_VALUE_FAMILY);
        append_u64(&mut bytes, 0u64);

        for (id, value) in metric_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }
        for (id, value) in label_name_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }
        for (id, value) in label_value_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }

        for (entry, family) in &series {
            append_u64(&mut bytes, entry.series_id);
            append_u32(&mut bytes, entry.metric_id);
            let pair_count = u16::try_from(entry.label_pairs.len()).map_err(|_| {
                TsinkError::InvalidConfiguration(
                    "series label pair count exceeds u16 in registry index".to_string(),
                )
            })?;
            append_u16(&mut bytes, pair_count);
            append_u16(&mut bytes, encode_optional_series_value_family(*family));
            for pair in &entry.label_pairs {
                append_u32(&mut bytes, pair.name_id);
                append_u32(&mut bytes, pair.value_id);
            }
        }

        let bytes = encode_optional_zstd_framed_file(&bytes)?;
        write_file_atomically_and_sync_parent(path, &bytes)?;
        Ok(())
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let raw_bytes = fs::read(path)?;
        let bytes = decode_optional_zstd_framed_file(
            &raw_bytes,
            REGISTRY_INDEX_MAGIC,
            REGISTRY_INDEX_VERSION,
            "series index",
        )?;
        if bytes.len() < 52 {
            return Err(TsinkError::DataCorruption(
                "series index file is too short".to_string(),
            ));
        }

        let mut pos = 0usize;
        let magic = read_array::<4>(&bytes, &mut pos)?;
        if magic != REGISTRY_INDEX_MAGIC {
            return Err(TsinkError::DataCorruption(
                "series index magic mismatch".to_string(),
            ));
        }

        let version = read_u16(&bytes, &mut pos)?;
        if version != REGISTRY_INDEX_VERSION {
            return Err(TsinkError::DataCorruption(format!(
                "unsupported series index version {version}"
            )));
        }

        let _flags = read_u16(&bytes, &mut pos)?;
        let next_series_id = read_u64(&bytes, &mut pos)?;
        let metric_count = read_u32(&bytes, &mut pos)? as usize;
        let label_name_count = read_u32(&bytes, &mut pos)? as usize;
        let label_value_count = read_u32(&bytes, &mut pos)? as usize;
        let series_count = read_u64(&bytes, &mut pos)? as usize;
        let section_flags = read_u64(&bytes, &mut pos)?;
        if section_flags & !REGISTRY_SECTION_VALUE_FAMILY != 0 {
            return Err(TsinkError::DataCorruption(format!(
                "invalid series index section flags {section_flags:#018x}"
            )));
        }
        let _reserved_postings_count = read_u64(&bytes, &mut pos)? as usize;

        let metric_values = parse_dictionary(&bytes, &mut pos, metric_count)?;
        let label_name_values = parse_dictionary(&bytes, &mut pos, label_name_count)?;
        let label_value_values = parse_dictionary(&bytes, &mut pos, label_value_count)?;

        let metric_dict = StringDictionary::from_values(metric_values, "metric")?;
        let label_name_dict = StringDictionary::from_values(label_name_values, "label name")?;
        let label_value_dict = StringDictionary::from_values(label_value_values, "label value")?;

        let mut definitions = Vec::with_capacity(series_count);
        for _ in 0..series_count {
            let series_id = read_u64(&bytes, &mut pos)?;
            let metric_id = read_u32(&bytes, &mut pos)?;
            let pair_count = read_u16(&bytes, &mut pos)? as usize;
            let value_family = if section_flags & REGISTRY_SECTION_VALUE_FAMILY != 0 {
                decode_optional_series_value_family(read_u16(&bytes, &mut pos)?)?
            } else {
                let _reserved = read_u16(&bytes, &mut pos)?;
                None
            };
            let mut label_pairs = Vec::with_capacity(pair_count);
            for _ in 0..pair_count {
                let name_id = read_u32(&bytes, &mut pos)?;
                let value_id = read_u32(&bytes, &mut pos)?;
                label_pairs.push(LabelPairId { name_id, value_id });
            }
            label_pairs.sort_unstable();
            label_pairs.dedup();
            definitions.push((
                SeriesDefinition {
                    series_id,
                    metric_id,
                    label_pairs,
                },
                value_family,
            ));
        }

        if pos != bytes.len() {
            return Err(TsinkError::DataCorruption(
                "series index has trailing bytes".to_string(),
            ));
        }

        let registry = Self {
            next_series_id: AtomicU64::new(next_series_id.max(1)),
            pending_series_reservations: AtomicUsize::new(0),
            estimated_total_bytes: AtomicUsize::new(0),
            postings_generation: AtomicU64::new(0),
            metric_dict: RwLock::new(metric_dict),
            label_name_dict: RwLock::new(label_name_dict),
            label_value_dict: RwLock::new(label_value_dict),
            series_shards: std::array::from_fn(|_| RwLock::new(SeriesRegistryShard::default())),
            series_id_shards: std::array::from_fn(|_| RwLock::new(SeriesIdShardIndex::default())),
            all_series_shards: std::array::from_fn(|_| {
                RwLock::new(AllSeriesPostingsShard::default())
            }),
            metric_postings_shards: std::array::from_fn(|_| {
                RwLock::new(MetricPostingsShard::default())
            }),
            label_postings_shards: std::array::from_fn(|_| {
                RwLock::new(LabelPostingsShard::default())
            }),
            series_count: AtomicUsize::new(definitions.len()),
        };
        for (definition, value_family) in definitions {
            let series_id = definition.series_id;
            let key = SeriesKeyIds {
                metric_id: definition.metric_id,
                label_pairs: definition.label_pairs.clone(),
            };
            let shard_idx = Self::key_shard_idx_for_key(&key);
            {
                let mut shard = registry.series_shards[shard_idx].write();
                shard.estimated_series_bytes = shard.estimated_series_bytes.saturating_add(
                    Self::series_label_pairs_memory_bytes(&definition.label_pairs),
                );
                if let Some(value_family) = value_family {
                    shard.value_families.insert(series_id, value_family);
                    shard.estimated_series_bytes = shard
                        .estimated_series_bytes
                        .saturating_add(Self::value_family_entry_bytes());
                }
                shard.by_key.insert(key, series_id);
                shard.by_id.insert(series_id, definition);
            }
            registry.series_id_shards[Self::series_id_index_shard_idx(series_id)]
                .write()
                .series_id_to_registry_shard
                .insert(series_id, shard_idx);
        }
        registry.rebuild_postings_indexes();
        Ok(registry)
    }

    fn load_optional_from_path(path: &Path) -> Result<Option<Self>> {
        match Self::load_from_path(path) {
            Ok(registry) => Ok(Some(registry)),
            Err(TsinkError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn load_incremental_segments(snapshot_path: &Path) -> Result<Option<LoadedSeriesRegistry>> {
        let segment_paths = Self::incremental_segment_paths(snapshot_path)?;
        if segment_paths.is_empty() {
            return Ok(None);
        }

        let mut loaded = LoadedSeriesRegistry {
            registry: Self::new(),
            delta_series_count: 0,
        };
        for path in segment_paths {
            let registry = Self::load_from_path(&path).map_err(|err| {
                TsinkError::DataCorruption(format!(
                    "failed to load incremental registry segment {}: {err}",
                    path.display()
                ))
            })?;
            loaded.delta_series_count = loaded
                .delta_series_count
                .saturating_add(registry.series_count());
            loaded.registry.merge_from(&registry).map_err(|err| {
                TsinkError::DataCorruption(format!(
                    "failed to merge incremental registry segment {}: {err}",
                    path.display()
                ))
            })?;
        }

        Ok(Some(loaded))
    }

    fn incremental_segment_paths(snapshot_path: &Path) -> Result<Vec<PathBuf>> {
        let dir_path = Self::incremental_dir(snapshot_path);
        match fs::symlink_metadata(&dir_path) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(TsinkError::DataCorruption(format!(
                        "incremental registry path is not a directory: {}",
                        dir_path.display()
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        }

        let mut paths = Vec::new();
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            if !name.starts_with(REGISTRY_INCREMENTAL_SEGMENT_PREFIX)
                || !name.ends_with(REGISTRY_INCREMENTAL_SEGMENT_SUFFIX)
            {
                continue;
            }
            paths.push(entry.path());
        }
        paths.sort();
        Ok(paths)
    }

    fn allocate_incremental_segment_path(dir_path: &Path) -> Result<PathBuf> {
        for _ in 0..256 {
            let nonce = REGISTRY_INCREMENTAL_SEGMENT_COUNTER.fetch_add(1, Ordering::Relaxed);
            let candidate = dir_path.join(format!(
                "{REGISTRY_INCREMENTAL_SEGMENT_PREFIX}{nonce:016x}{REGISTRY_INCREMENTAL_SEGMENT_SUFFIX}"
            ));
            if !path_exists_no_follow(&candidate)? {
                return Ok(candidate);
            }
        }

        Err(TsinkError::Other(format!(
            "failed to allocate incremental registry segment in {}",
            dir_path.display()
        )))
    }

    fn merge_loaded_registry(
        target: &mut Option<LoadedSeriesRegistry>,
        incoming: LoadedSeriesRegistry,
    ) -> Result<()> {
        if let Some(existing) = target.as_mut() {
            existing.registry.merge_from(&incoming.registry)?;
            existing.delta_series_count = existing
                .delta_series_count
                .saturating_add(incoming.delta_series_count);
            return Ok(());
        }

        *target = Some(incoming);
        Ok(())
    }
}

fn write_dict_entry(out: &mut Vec<u8>, id: u32, value: &str) -> Result<()> {
    let value_bytes = value.as_bytes();
    let len = u32::try_from(value_bytes.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("registry dictionary string exceeds u32".to_string())
    })?;
    append_u32(out, id);
    append_u32(out, len);
    out.extend_from_slice(value_bytes);
    Ok(())
}

fn parse_dictionary(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<String>> {
    let mut values = Vec::with_capacity(count);
    for expected_id in 0..count {
        let id = read_u32(bytes, pos)? as usize;
        if id != expected_id {
            return Err(TsinkError::DataCorruption(format!(
                "registry dictionary id {} is not dense at expected {}",
                id, expected_id
            )));
        }

        let len = read_u32(bytes, pos)? as usize;
        let value_bytes = read_bytes(bytes, pos, len)?;
        let value = String::from_utf8(value_bytes.to_vec())?;
        values.push(value);
    }
    Ok(values)
}
