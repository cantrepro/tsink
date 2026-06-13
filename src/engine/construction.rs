use super::*;

#[path = "construction/assembly.rs"]
mod assembly;
#[path = "construction/resources.rs"]
mod resources;

use self::assembly::StorageStateAssembly;
use self::resources::StorageAssemblyResources;

pub(super) use self::resources::{PendingPersistedSegmentDiff, RemoteCatalogRefreshState};

impl ChunkStorage {
    pub fn try_new(chunk_point_cap: usize, wal: Option<FramedWal>) -> Result<Self> {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            None,
            None,
            1,
            ChunkStorageOptions::default(),
        )
    }

    pub fn try_new_with_data_path(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
    ) -> Result<Self> {
        Self::reject_existing_storage_state(
            numeric_lane_path.as_deref(),
            blob_lane_path.as_deref(),
        )?;
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            numeric_lane_path,
            blob_lane_path,
            next_segment_id,
            ChunkStorageOptions::default(),
        )
    }

    // Public callers should go through StorageBuilder::build so durable startup
    // always runs recovery and returns structured initialization errors.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(chunk_point_cap: usize, wal: Option<FramedWal>) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            None,
            None,
            1,
            ChunkStorageOptions::default(),
        )
        .expect("failed to initialize chunk storage")
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_data_path(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
    ) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            numeric_lane_path,
            blob_lane_path,
            next_segment_id,
            ChunkStorageOptions::default(),
        )
        .expect("failed to initialize chunk storage")
    }

    fn reject_existing_storage_state(
        numeric_lane_path: Option<&Path>,
        blob_lane_path: Option<&Path>,
    ) -> Result<()> {
        for lane_path in [numeric_lane_path, blob_lane_path].into_iter().flatten() {
            if Self::path_contains_storage_entries(lane_path)? {
                return Err(Self::existing_storage_requires_builder_error());
            }
        }

        let Some(data_path) = numeric_lane_path
            .and_then(Path::parent)
            .or_else(|| blob_lane_path.and_then(Path::parent))
        else {
            return Ok(());
        };

        let series_index_path = data_path.join(SERIES_INDEX_FILE_NAME);
        if crate::engine::fs_utils::path_exists_no_follow(&series_index_path)? {
            return Err(Self::existing_storage_requires_builder_error());
        }

        let wal_path = data_path.join(WAL_DIR_NAME);
        if Self::path_contains_storage_entries(&wal_path)? {
            return Err(Self::existing_storage_requires_builder_error());
        }

        Ok(())
    }

    fn path_contains_storage_entries(path: &Path) -> Result<bool> {
        let mut entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err.into()),
        };

        if let Some(entry) = entries.next() {
            entry?;
            return Ok(true);
        }

        Ok(false)
    }

    fn existing_storage_requires_builder_error() -> TsinkError {
        TsinkError::InvalidConfiguration(
            "opening existing on-disk ChunkStorage state requires StorageBuilder::build() so startup recovery can run"
                .to_string(),
        )
    }

    pub(super) fn new_with_data_path_and_options(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
        options: ChunkStorageOptions,
    ) -> Result<Self> {
        let resources = Self::prepare_construction_resources(
            chunk_point_cap,
            numeric_lane_path.as_ref(),
            blob_lane_path.as_ref(),
            next_segment_id,
            &options,
        )?;
        let storage_state = StorageStateAssembly::build(
            chunk_point_cap,
            numeric_lane_path,
            blob_lane_path,
            wal,
            &options,
            resources,
        );
        let StorageStateAssembly {
            catalog,
            chunks,
            visibility,
            persisted,
            runtime,
            memory,
            coordination,
            background,
            rollups,
            observability,
        } = storage_state;

        let storage = Self {
            catalog,
            chunks,
            visibility,
            persisted,
            runtime,
            memory,
            coordination,
            background,
            rollups,
            observability,
            #[cfg(test)]
            current_time_override: AtomicI64::new(
                options.current_time_override.unwrap_or(i64::MIN),
            ),
            #[cfg(test)]
            persist_test_hooks: PersistTestHooks::default(),
        };

        storage.prime_wal_registry_snapshot();
        if storage.memory.accounting_enabled {
            storage.refresh_memory_usage();
        }

        Ok(storage)
    }

    fn prime_wal_registry_snapshot(&self) {
        if let Some(wal) = &self.persisted.wal {
            wal.prime_committed_series_definitions_snapshot(std::iter::empty::<
                crate::engine::wal::SeriesDefinitionFrame,
            >());
        }
    }
}
