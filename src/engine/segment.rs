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
#[cfg(test)]
pub(in crate::engine) use self::writer::fail_segment_publish_rollback_once;
pub use self::writer::SegmentWriter;

pub(crate) use self::format::chunk_payload_from_record;
pub(crate) use self::format::decoded_chunks_file_payload_bytes;
#[cfg(test)]
pub(crate) use self::format::CHUNK_FLAG_PAYLOAD_ZSTD;
pub(crate) use self::format::{MAX_SEGMENT_CHUNKS_FILE_BYTES, MAX_SEGMENT_MANIFEST_FILE_BYTES};

pub(crate) use self::loader::{
    load_segment_indexes_from_dirs_startup_recoverable_with_series, load_segment_series_metadata,
    load_segments_runtime_strict, read_segment_manifest_fingerprint,
    validate_legacy_segment_identity, validate_segment_payloads_for_restore,
    verify_segment_fingerprint, visit_segment_dirs_with_namespace_budget,
};
pub(crate) use self::types::{SegmentContentFingerprint, SegmentFileFingerprint};
pub(crate) use self::validation::{
    is_not_found_error, load_complete_segment_no_follow, quarantine_invalid_startup_segment,
    quarantine_segment_root, runtime_refresh_disappearance_error, segment_validation_error,
    segment_validation_error_message, SegmentValidationContext, StartupQuarantinedSegment,
};
#[cfg(test)]
pub(crate) use self::validation::{QuarantinedSegmentRoot, STARTUP_SEGMENT_QUARANTINE_PURPOSE};
