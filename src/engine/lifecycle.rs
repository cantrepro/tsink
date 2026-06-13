//! Lifecycle orchestration for flush, replay, persisted-state install, and shutdown.
//!
//! This parent stays as the lifecycle entry point while detailed stages live
//! under `src/engine/lifecycle/`.

#[path = "lifecycle/flush.rs"]
mod flush;
#[path = "lifecycle/persisted_state.rs"]
mod persisted_state;
#[path = "lifecycle/replay.rs"]
mod replay;
#[path = "lifecycle/shutdown.rs"]
mod shutdown;

use super::{
    elapsed_nanos_u64, saturating_u64_from_usize, state, ActiveSeriesState, Arc, Chunk,
    ChunkStorage, Compactor, HashMap, Instant, Label, LifecyclePublicationContext, Ordering, Path,
    PathBuf, PendingPersistedSegmentDiff, PersistedChunkRef, PersistedIndexState, Result,
    SegmentWriter, SeriesId, SeriesRegistry, SeriesValueFamily, StorageObservabilityCounters,
    TsinkError, ValueLane, WalHighWatermark, CLOSE_COMPACTION_MAX_PASSES,
};
