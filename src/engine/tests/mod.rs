//! Shared engine test scaffolding and contributor guardrails.
//!
//! Prefer the smallest fixture that can expose the invariant under test:
//! - pure ingest or in-memory behavior should avoid a data path entirely
//! - persistence and recovery tests should reuse the persistent storage helpers
//! - rollup and dual-lane tests should opt into both lanes explicitly instead of
//!   open-coding a larger `ChunkStorageOptions` block
//!
//! CI mirrors these boundaries with focused `cargo test --lib engine::tests::...`
//! filters in `.github/workflows/ci.yml`. When a refactor changes an engine
//! invariant, tighten the narrowest affected suite before broadening an
//! end-to-end fixture.
//!
//! `src/engine/tests/mod.rs` is the shared test prelude and suite registry.
//! Reusable storage-shape fixtures live under `src/engine/tests/fixtures/`.

pub(super) use std::collections::HashMap;
pub(super) use std::path::Path;
pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;

pub(super) use tempfile::TempDir;

pub(super) use super::bootstrap::merge_loaded_segment_indexes;
pub(super) use super::{
    ChunkStorage, ChunkStorageOptions, BLOB_LANE_ROOT, DEFAULT_ADMISSION_POLL_INTERVAL,
    DEFAULT_COMPACTION_INTERVAL, NUMERIC_LANE_ROOT, SERIES_INDEX_FILE_NAME, WAL_DIR_NAME,
};
pub(super) use crate::engine::chunk::{
    Chunk, ChunkHeader, ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane,
};
pub(super) use crate::engine::encoder::Encoder;
pub(super) use crate::engine::segment::{
    load_segment_index, load_segment_indexes, load_segments_for_level, SegmentWriter,
    WalHighWatermark,
};
pub(super) use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
pub(super) use crate::engine::wal::{
    FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame,
};
pub(super) use crate::wal::{WalReplayMode, WalSyncMode};
pub(super) use crate::{
    DataPoint, Label, MetricSeries, RemoteSegmentCachePolicy, Row, SeriesMatcher, SeriesPoints,
    SeriesSelection, Storage, StorageBuilder, StorageRuntimeMode, TimestampPrecision, TsinkError,
    Value,
};

#[path = "fixtures/mod.rs"]
mod fixtures;

pub(in crate::engine::storage_engine::tests) use self::fixtures::{
    base_storage_test_options, builder_at_time, default_future_skew_window,
    live_numeric_storage_with_config, make_persisted_numeric_chunk,
    open_raw_numeric_storage_from_data_path,
    open_raw_numeric_storage_from_data_path_with_background_fail_fast,
    open_raw_numeric_storage_with_registry_snapshot_from_data_path, persistent_numeric_storage,
    persistent_numeric_storage_with_metadata_shards, persistent_rollup_storage,
    reopen_persistent_numeric_storage, reopen_persistent_rollup_storage,
    reopen_persistent_rollup_storage_with_disk_budget,
};

mod admission_control;
mod capacity;
mod deletion;
mod failure_modes;
mod ingest_concurrency;
mod ingest_core;
mod ingest_failures;
mod persistence_background;
mod persistence_recovery;
mod persistence_segments;
mod query_budget;
mod reliability;
mod resource_profiles;
mod retention_policy;
mod rollup_write_batching;
mod rollups;
mod series_selection;
mod write_transient_memory;

pub(crate) fn assert_engine_memory_usage_reconciled(storage: &ChunkStorage) {
    if !storage.memory.accounting_enabled {
        return;
    }

    let incremental = storage.memory_observability_snapshot();
    let reconciled_total = storage.refresh_memory_usage();
    let reconciled = storage.memory_observability_snapshot();

    assert_eq!(
        incremental.budgeted_bytes, reconciled_total,
        "incremental engine memory accounting drifted from a full recompute: incremental={incremental:?} reconciled={reconciled:?}",
    );
    assert_eq!(incremental.budgeted_bytes, reconciled.budgeted_bytes);
    assert_eq!(incremental.excluded_bytes, reconciled.excluded_bytes);
    assert_eq!(
        incremental.active_and_sealed_bytes,
        reconciled.active_and_sealed_bytes
    );
    assert_eq!(incremental.registry_bytes, reconciled.registry_bytes);
    assert_eq!(
        incremental.metadata_cache_bytes,
        reconciled.metadata_cache_bytes
    );
    assert_eq!(
        incremental.persisted_index_bytes,
        reconciled.persisted_index_bytes
    );
    assert_eq!(
        incremental.persisted_mmap_bytes,
        reconciled.persisted_mmap_bytes
    );
    assert_eq!(incremental.tombstone_bytes, reconciled.tombstone_bytes);
    assert_eq!(
        incremental.wal_series_definition_cache_bytes,
        reconciled.wal_series_definition_cache_bytes
    );
    assert_eq!(
        incremental.excluded_persisted_mmap_bytes,
        reconciled.excluded_persisted_mmap_bytes
    );
}

/// Raw `ChunkStorage` fixtures bypass the builder bootstrap that acquires and installs the
/// shared object-store writer lease. Read-write tiering tests must install the same production
/// fence explicitly; compute-only fixtures intentionally do not call this helper.
pub(super) fn install_shared_object_store_writer_lock_for_test(
    storage: &ChunkStorage,
    object_store_root: &Path,
) {
    let writer_lock = super::process_lock::SharedObjectStoreProcessLock::acquire(object_store_root)
        .expect("test fixture should acquire the shared object-store writer lease");
    storage.install_shared_object_store_process_lock(writer_lock);
}
