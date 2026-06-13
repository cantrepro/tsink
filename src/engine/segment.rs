mod format;
mod loader;
mod postings;
#[cfg(test)]
mod tests;
mod types;
mod validation;
mod writer;

pub use crate::engine::durability::WalHighWatermark;

pub use self::loader::{
    collect_expired_segment_dirs, list_segment_dirs, load_segment, load_segment_index,
    load_segment_index_with_series, load_segment_indexes,
    load_segment_indexes_from_dirs_strict_with_series, load_segment_indexes_from_dirs_with_series,
    load_segment_indexes_with_series, load_segments, load_segments_for_level,
    read_segment_manifest,
};
pub(crate) use self::postings::SegmentPostingsIndex;
pub use self::types::{
    IndexedSegment, LoadedSegment, LoadedSegmentIndexes, LoadedSegments, PersistedSeries,
    SegmentLayout, SegmentManifest,
};
pub use self::writer::SegmentWriter;

pub(crate) use self::format::chunk_payload_from_record;
#[cfg(test)]
pub(crate) use self::format::CHUNK_FLAG_PAYLOAD_ZSTD;

pub(crate) use self::loader::{
    load_segment_indexes_from_dirs_startup_recoverable_with_series, load_segment_series_metadata,
    load_segments_for_level_runtime_strict, load_segments_runtime_strict,
    read_segment_manifest_fingerprint, verify_segment_fingerprint,
};
pub(crate) use self::types::SegmentContentFingerprint;
pub(crate) use self::validation::{
    is_not_found_error, quarantine_invalid_startup_segment, quarantine_segment_root,
    runtime_refresh_disappearance_error, segment_validation_error,
    segment_validation_error_message, SegmentValidationContext, StartupQuarantinedSegment,
};
#[cfg(test)]
pub(crate) use self::validation::{QuarantinedSegmentRoot, STARTUP_SEGMENT_QUARANTINE_PURPOSE};
