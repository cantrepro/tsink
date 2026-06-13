use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::RwLock;
use roaring::RoaringTreemap;

use crate::Label;

mod dictionary;
mod identity;
mod persistence;
mod postings;
#[cfg(test)]
mod tests;
mod value_family;

pub type SeriesId = u64;
pub type DictionaryId = u32;

const REGISTRY_INDEX_MAGIC: [u8; 4] = *b"RIDX";
const REGISTRY_INDEX_VERSION: u16 = 2;
const REGISTRY_SECTION_VALUE_FAMILY: u64 = 0b0000_0001;
pub const REGISTRY_INCREMENTAL_FILE_NAME: &str = "series_index.delta.bin";
pub const REGISTRY_INCREMENTAL_DIR_NAME: &str = "series_index.delta.d";
const REGISTRY_INCREMENTAL_SEGMENT_PREFIX: &str = "delta-";
const REGISTRY_INCREMENTAL_SEGMENT_SUFFIX: &str = ".bin";
static REGISTRY_INCREMENTAL_SEGMENT_COUNTER: AtomicU64 = AtomicU64::new(1);
const SERIES_REGISTRY_SHARD_COUNT: usize = 64;

#[derive(Debug)]
pub struct LoadedSeriesRegistry {
    pub registry: SeriesRegistry,
    pub delta_series_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SeriesValueFamily {
    F64,
    I64,
    U64,
    Bool,
    Blob,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LabelPairId {
    pub name_id: DictionaryId,
    pub value_id: DictionaryId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKeyIds {
    metric_id: DictionaryId,
    label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDefinition {
    pub series_id: SeriesId,
    pub metric_id: DictionaryId,
    pub label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesKey {
    pub metric: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesResolution {
    pub series_id: SeriesId,
    pub metric_id: DictionaryId,
    pub label_pairs: Vec<LabelPairId>,
    pub created: bool,
}

#[derive(Debug, Default)]
struct StringDictionary {
    by_value: HashMap<String, DictionaryId>,
    by_id: Vec<String>,
    estimated_heap_bytes: usize,
}

#[derive(Debug, Default)]
struct SeriesRegistryShard {
    by_key: HashMap<SeriesKeyIds, SeriesId>,
    by_id: HashMap<SeriesId, SeriesDefinition>,
    value_families: HashMap<SeriesId, SeriesValueFamily>,
    estimated_series_bytes: usize,
}

#[derive(Debug, Default)]
struct SeriesIdShardIndex {
    series_id_to_registry_shard: HashMap<SeriesId, usize>,
}

#[derive(Debug, Default)]
struct AllSeriesPostingsShard {
    series_ids: RoaringTreemap,
    estimated_postings_bytes: usize,
}

#[derive(Debug, Clone)]
struct MissingLabelPostingsCacheEntry {
    bitmap: RoaringTreemap,
    postings_generation: u64,
}

#[derive(Debug, Default, Clone)]
struct LabelNamePostingsState {
    present: RoaringTreemap,
    missing_cache: Option<MissingLabelPostingsCacheEntry>,
    bucket_count: usize,
}

#[derive(Debug, Default)]
struct MetricPostingsShard {
    metric_postings: HashMap<DictionaryId, RoaringTreemap>,
    estimated_postings_bytes: usize,
}

#[derive(Debug, Default)]
struct LabelPostingsShard {
    label_name_states: HashMap<DictionaryId, LabelNamePostingsState>,
    postings: BTreeMap<LabelPairId, RoaringTreemap>,
    estimated_postings_bytes: usize,
}

#[derive(Debug)]
pub struct SeriesRegistry {
    next_series_id: AtomicU64,
    pending_series_reservations: AtomicUsize,
    estimated_total_bytes: AtomicUsize,
    postings_generation: AtomicU64,
    metric_dict: RwLock<StringDictionary>,
    label_name_dict: RwLock<StringDictionary>,
    label_value_dict: RwLock<StringDictionary>,
    series_shards: [RwLock<SeriesRegistryShard>; SERIES_REGISTRY_SHARD_COUNT],
    series_id_shards: [RwLock<SeriesIdShardIndex>; SERIES_REGISTRY_SHARD_COUNT],
    all_series_shards: [RwLock<AllSeriesPostingsShard>; SERIES_REGISTRY_SHARD_COUNT],
    metric_postings_shards: [RwLock<MetricPostingsShard>; SERIES_REGISTRY_SHARD_COUNT],
    label_postings_shards: [RwLock<LabelPostingsShard>; SERIES_REGISTRY_SHARD_COUNT],
    series_count: AtomicUsize,
}

impl Default for SeriesRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SeriesRegistry {
    pub fn new() -> Self {
        Self {
            next_series_id: AtomicU64::new(1),
            pending_series_reservations: AtomicUsize::new(0),
            estimated_total_bytes: AtomicUsize::new(0),
            postings_generation: AtomicU64::new(0),
            metric_dict: RwLock::new(StringDictionary::default()),
            label_name_dict: RwLock::new(StringDictionary::default()),
            label_value_dict: RwLock::new(StringDictionary::default()),
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
            series_count: AtomicUsize::new(0),
        }
    }

    fn series_label_pairs_memory_bytes(label_pairs: &[LabelPairId]) -> usize {
        label_pairs
            .len()
            .saturating_mul(std::mem::size_of::<LabelPairId>())
            .saturating_mul(2)
    }

    pub(crate) fn value_family_entry_bytes() -> usize {
        std::mem::size_of::<SeriesId>() + std::mem::size_of::<SeriesValueFamily>()
    }

    fn label_name_postings_count_entry_bytes() -> usize {
        std::mem::size_of::<DictionaryId>() + std::mem::size_of::<usize>()
    }

    fn bitmap_memory_usage_bytes(bitmap: &RoaringTreemap) -> usize {
        if bitmap.is_empty() {
            0
        } else {
            bitmap.serialized_size()
        }
    }

    fn recompute_series_bytes(&self) -> usize {
        self.series_shards.iter().fold(0usize, |acc, shard| {
            acc.saturating_add(shard.read().estimated_series_bytes)
        })
    }

    fn recompute_postings_bytes(&self) -> usize {
        let all_series_bytes = self.all_series_shards.iter().fold(0usize, |acc, shard| {
            acc.saturating_add(shard.read().estimated_postings_bytes)
        });
        let metric_bytes = self
            .metric_postings_shards
            .iter()
            .fold(0usize, |acc, shard| {
                acc.saturating_add(shard.read().estimated_postings_bytes)
            });
        let label_bytes = self
            .label_postings_shards
            .iter()
            .fold(0usize, |acc, shard| {
                acc.saturating_add(shard.read().estimated_postings_bytes)
            });
        all_series_bytes
            .saturating_add(metric_bytes)
            .saturating_add(label_bytes)
    }

    fn key_shard_idx_for_key(key: &SeriesKeyIds) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % SERIES_REGISTRY_SHARD_COUNT
    }

    fn metric_postings_shard_idx(metric_id: DictionaryId) -> usize {
        (metric_id as usize) % SERIES_REGISTRY_SHARD_COUNT
    }

    fn label_postings_shard_idx(name_id: DictionaryId) -> usize {
        (name_id as usize) % SERIES_REGISTRY_SHARD_COUNT
    }

    fn all_series_shard_idx(series_id: SeriesId) -> usize {
        (series_id as usize) % SERIES_REGISTRY_SHARD_COUNT
    }

    fn series_id_index_shard_idx(series_id: SeriesId) -> usize {
        (series_id as usize) % SERIES_REGISTRY_SHARD_COUNT
    }

    fn next_series_id_value(&self) -> SeriesId {
        self.next_series_id.load(Ordering::Acquire).max(1)
    }

    fn load_series_registry_shard_idx(&self, series_id: SeriesId) -> Option<usize> {
        self.series_id_shards[Self::series_id_index_shard_idx(series_id)]
            .read()
            .series_id_to_registry_shard
            .get(&series_id)
            .copied()
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.estimated_total_bytes.load(Ordering::Acquire)
    }

    fn recompute_memory_usage_bytes(&self) -> usize {
        self.metric_dict
            .read()
            .memory_usage_bytes()
            .saturating_add(self.label_name_dict.read().memory_usage_bytes())
            .saturating_add(self.label_value_dict.read().memory_usage_bytes())
            .saturating_add(self.recompute_series_bytes())
            .saturating_add(self.recompute_postings_bytes())
    }

    fn add_estimated_memory_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let mut current = self.estimated_total_bytes.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(bytes);
            match self.estimated_total_bytes.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn bump_postings_generation(&self) {
        self.postings_generation.fetch_add(1, Ordering::AcqRel);
    }

    fn sub_estimated_memory_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let mut current = self.estimated_total_bytes.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(bytes);
            match self.estimated_total_bytes.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn refresh_estimated_memory_usage_bytes(&self) -> usize {
        let total = self.recompute_memory_usage_bytes();
        self.estimated_total_bytes.store(total, Ordering::Release);
        total
    }
}
