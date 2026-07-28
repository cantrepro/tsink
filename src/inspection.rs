//! Bounded, read-only inspection of a tsink data directory.
//!
//! Inspection deliberately does not open [`crate::Storage`], acquire the process lock, run
//! recovery, or use WAL salvage mode. Every filesystem and report-retention limit is explicit in
//! [`DataDirectoryInspectionLimits`]. A report whose [`InspectionCompleteness::complete`] field is
//! false must not be interpreted as a clean bill of health.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ffi::OsStr;
#[cfg(unix)]
use std::ffi::{CStr, CString, OsString};
use std::fs::{File, Metadata, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use xxhash_rust::xxh64::Xxh64;

use crate::engine::segment::WalHighWatermark;
use crate::engine::wal::{
    decode_published_highwater_record, encode_published_highwater_record, PublishedHighwaterRecord,
};
use crate::{Result, TsinkError};

/// Schema version of [`DataDirectoryInspectionReport`].
pub const DATA_DIRECTORY_INSPECTION_REPORT_SCHEMA_VERSION: u16 = 1;

const DATA_DIRECTORY_MANIFEST_FILE_NAME: &str = "tsink-manifest.json";
const SALVAGE_REPORT_FILE_NAME: &str = "tsink-salvage-report.json";
const DATA_DIRECTORY_MAGIC: &str = "TSINK_DATA_DIRECTORY";
const DATA_DIRECTORY_MANIFEST_SCHEMA_VERSION: u16 = 1;
const DATA_DIRECTORY_MANIFEST_MAX_BYTES: u64 = 16 * 1024;
const CURRENT_FORMAT_FEATURES: [&str; 4] = [
    "blob_value_lane_v1",
    "framed_segment_v2",
    "framed_wal_v2",
    "registry_snapshot_v2",
];
const WAL_FRAME_MAGIC: [u8; 4] = *b"TSFR";
const WAL_FRAME_HEADER_BYTES: u64 = 24;
const WAL_MAX_FRAME_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const WAL_PUBLISHED_LEGACY_BYTES: u64 = 24;
const WAL_PUBLISHED_V2_BYTES: u64 = 40;
const WAL_PUBLISHED_MAX_BYTES: u64 = WAL_PUBLISHED_V2_BYTES;
const SEGMENT_MANIFEST_MAGIC: [u8; 4] = *b"TSM2";
const SEGMENT_FORMAT_VERSION: u16 = 2;
const SEGMENT_MANIFEST_BYTES: usize = 180;
const HASH_BUFFER_BYTES: usize = 16 * 1024;
const TRUNCATED_PATH_SUFFIX: &str = "%TRUNCATED";
const MAX_INSPECTION_NAMESPACE_DEPTH: u16 = 128;
#[cfg(unix)]
static SALVAGE_STAGE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Finite work and retained-output limits for one inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataDirectoryInspectionLimits {
    /// Maximum filesystem entries admitted across the source namespace.
    pub max_namespace_entries: u64,
    /// Maximum directory depth below the source root.
    pub max_namespace_depth: u16,
    /// Maximum modeled bytes retained for discovered namespace entries and sortable names.
    pub max_namespace_retained_bytes: u64,
    /// Maximum bytes read from regular files, including bytes also hashed.
    pub max_bytes_read: u64,
    /// Maximum file-content bytes fed to a content hash.
    pub max_bytes_hashed: u64,
    /// Maximum retained findings, WAL records, segment records, and discarded ranges.
    pub max_report_items: u64,
    /// Maximum retained bytes across all rendered source-relative paths.
    pub max_retained_path_bytes: u64,
    /// Maximum encoded bytes retained for one rendered path.
    pub max_path_bytes: u32,
    /// Maximum retained findings.
    pub max_issues: u64,
    /// Maximum WAL frames verified across all canonical WAL segments.
    pub max_wal_frames: u64,
    /// Maximum modeled decoded heap bytes for one WAL frame.
    pub max_wal_decoded_bytes: u64,
    /// Maximum canonical WAL segment files verified.
    pub max_wal_segments: u64,
    /// Maximum canonical persisted segment directories verified.
    pub max_segments: u64,
    /// Maximum bytes retained while decoding one JSON catalog.
    pub max_catalog_bytes: u64,
    /// Maximum modeled peak bytes retained while validating the series-registry snapshot.
    pub max_registry_bytes: u64,
}

impl Default for DataDirectoryInspectionLimits {
    fn default() -> Self {
        Self {
            max_namespace_entries: 100_000,
            max_namespace_depth: 16,
            max_namespace_retained_bytes: 128 * 1024 * 1024,
            max_bytes_read: 8 * 1024 * 1024 * 1024,
            max_bytes_hashed: 8 * 1024 * 1024 * 1024,
            max_report_items: 100_000,
            max_retained_path_bytes: 16 * 1024 * 1024,
            max_path_bytes: 4 * 1024,
            max_issues: 10_000,
            max_wal_frames: 10_000_000,
            max_wal_decoded_bytes: 256 * 1024 * 1024,
            max_wal_segments: 100_000,
            max_segments: 100_000,
            max_catalog_bytes: 64 * 1024 * 1024,
            max_registry_bytes: 256 * 1024 * 1024,
        }
    }
}

impl DataDirectoryInspectionLimits {
    fn validate(self) -> Result<Self> {
        if self.max_namespace_depth > MAX_INSPECTION_NAMESPACE_DEPTH {
            return Err(TsinkError::InvalidConfiguration(format!(
                "max_namespace_depth must be at most {MAX_INSPECTION_NAMESPACE_DEPTH}"
            )));
        }
        if self.max_namespace_entries == 0
            || self.max_bytes_read == 0
            || self.max_bytes_hashed == 0
            || self.max_report_items == 0
            || self.max_retained_path_bytes < 32
            || self.max_path_bytes < 32
            || self.max_issues == 0
            || self.max_wal_frames == 0
            || self.max_wal_decoded_bytes == 0
            || self.max_wal_segments == 0
            || self.max_segments == 0
            || self.max_catalog_bytes == 0
            || self.max_registry_bytes == 0
            || self.max_namespace_retained_bytes == 0
        {
            return Err(TsinkError::InvalidConfiguration(
                "data-directory inspection limits must be non-zero and path limits must be at least 32 bytes"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

/// High-level format identity determined without mutating or normally opening the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataDirectoryFormat {
    /// The namespace has no durable tsink or foreign entries.
    Empty,
    /// A current, checksum-valid data-directory manifest identifies the namespace.
    CurrentManifest,
    /// A manifestless namespace has a valid v2 WAL, segment, or registry identity.
    SupportedPreManifestV2,
    /// A readable manifest requires a newer schema, storage format, or reader.
    UnsupportedNewer,
    /// A tsink identity record is present but malformed or checksum-invalid.
    Corrupt,
    /// No definitive tsink format identity was found.
    UnknownForeign,
}

/// Report health. `Clean` is possible only for a complete inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionHealth {
    /// All requested checks completed and no warning or error finding was retained.
    Clean,
    /// All requested checks completed but at least one finding was present.
    FindingsPresent,
    /// At least one work or report bound prevented a complete inspection.
    Incomplete,
}

/// Stable severity attached to a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionSeverity {
    /// Informational state that does not itself indicate corruption.
    Info,
    /// Suspicious or unverifiable state.
    Warning,
    /// Definite corruption, invalid namespace state, or a missing referenced object.
    Error,
}

/// A stable machine-readable finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionFinding {
    /// Stable dotted identifier suitable for automation.
    pub code: String,
    /// Finding severity.
    pub severity: InspectionSeverity,
    /// Bounded, source-relative path, when one can be safely retained.
    pub path: Option<String>,
    /// Bounded explanatory text. Automation should branch on `code`, not this text.
    pub message: String,
    /// A byte range relevant to the finding.
    pub byte_range: Option<InspectionByteRange>,
}

/// Half-open byte range `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionByteRange {
    /// Inclusive start offset.
    pub start: u64,
    /// Exclusive end offset.
    pub end: u64,
}

/// A range that a future destination-only salvage action would have to discard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionDiscardedRange {
    /// Bounded source-relative path.
    pub path: String,
    /// Half-open byte range.
    pub bytes: InspectionByteRange,
    /// First affected frame sequence when known.
    pub first_frame: Option<u64>,
    /// Last affected frame sequence when known.
    pub last_frame: Option<u64>,
    /// Stable reason code.
    pub reason_code: String,
}

/// A finite work counter snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionWork {
    /// Namespace entries admitted.
    pub namespace_entries: u64,
    /// Directories whose entries were enumerated.
    pub directories_visited: u64,
    /// Modeled bytes retained for discovered namespace entries and sortable names.
    pub namespace_retained_bytes: u64,
    /// Bytes read.
    pub bytes_read: u64,
    /// Bytes hashed.
    pub bytes_hashed: u64,
    /// WAL frames whose complete header and payload were verified.
    pub wal_frames: u64,
    /// WAL segment files admitted.
    pub wal_segments: u64,
    /// Persisted segment directories admitted.
    pub segments: u64,
    /// Modeled peak bytes admitted for series-registry decoding.
    pub registry_modeled_bytes: u64,
    /// Retained report items.
    pub report_items: u64,
    /// Retained rendered-path bytes.
    pub retained_path_bytes: u64,
    /// Retained findings.
    pub issues: u64,
}

/// Completeness and exact bound names hit during inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionCompleteness {
    /// True only when no limit prevented a requested check or report item.
    pub complete: bool,
    /// Stable field names of every bound hit, in deterministic order.
    pub bounds_hit: Vec<String>,
}

/// Readable manifest fields, retained even when the checksum or compatibility check fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedDataDirectoryManifest {
    /// Manifest magic, when readable.
    pub magic: Option<String>,
    /// Envelope schema version, when readable.
    pub manifest_schema_version: Option<u16>,
    /// Storage format version, when readable.
    pub storage_format_version: Option<u16>,
    /// Minimum reader storage format, when readable.
    pub minimum_reader_storage_format_version: Option<u16>,
    /// Creating tsink release, when present.
    pub creating_tsink_version: Option<String>,
    /// Last release that completed startup, when present.
    pub last_successfully_opened_tsink_version: Option<String>,
    /// Format-affecting feature identifiers.
    pub format_affecting_features: Vec<String>,
    /// Timestamp precision string.
    pub timestamp_precision: Option<String>,
    /// Immutable chunk capacity.
    pub chunk_point_capacity: Option<u32>,
    /// Immutable partition window in configured timestamp units.
    pub partition_window_timestamp_units: Option<i64>,
    /// Whether canonical payload CRC32 validation succeeded.
    pub checksum_valid: Option<bool>,
}

/// WAL segment corruption classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalSegmentStatus {
    /// Every frame was structurally complete and checksum-valid.
    Clean,
    /// The final frame header or payload was truncated.
    CorruptTail,
    /// A complete frame had invalid magic, type, ordering, or checksum.
    MidLogCorruption,
    /// A bound prevented complete verification.
    Incomplete,
}

/// One canonical WAL segment result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedWalSegment {
    /// Canonical WAL segment id.
    pub segment_id: u64,
    /// Bounded relative path.
    pub path: String,
    /// Physical length.
    pub file_len: u64,
    /// Number of complete checksum-valid frames.
    pub valid_frames: u64,
    /// First checksum-valid frame sequence in this file.
    pub first_valid_frame: Option<u64>,
    /// Last checksum-valid frame sequence.
    pub last_valid_frame: Option<u64>,
    /// Type byte of the first checksum-valid frame in this file.
    pub first_valid_frame_type: Option<u8>,
    /// Type byte of the last checksum-valid frame in this file.
    pub last_valid_frame_type: Option<u8>,
    /// Corruption or completeness state.
    pub status: WalSegmentStatus,
}

/// Strict validation result for the canonical series-registry snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedSeriesRegistry {
    /// Whether `series_index.bin` exists.
    pub present: bool,
    /// Whether the production decoder accepted the complete bounded snapshot.
    pub valid: bool,
    /// Decoded series count when validation succeeded.
    pub series_count: Option<u64>,
}

/// Published WAL high-water marker state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedWalPublishedMarker {
    /// Whether the marker exists.
    pub present: bool,
    /// Whether its exact size, magic, and checksum are valid.
    pub checksum_valid: Option<bool>,
    /// Published segment id.
    pub segment: Option<u64>,
    /// Published frame sequence.
    pub frame: Option<u64>,
    /// Segment id through which a checksummed v2 marker authorizes reset WAL absence.
    #[serde(default)]
    pub reset_through_segment: Option<u64>,
    /// Frame sequence through which a checksummed v2 marker authorizes reset WAL absence.
    #[serde(default)]
    pub reset_through_frame: Option<u64>,
    /// Type byte of the exact published frame, when nonzero and verified.
    pub frame_type: Option<u8>,
}

/// Persisted segment verification state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedSegmentStatus {
    /// Manifest and all four referenced files match.
    Clean,
    /// At least one required object is missing.
    MissingFiles,
    /// A manifest, length, or content checksum is invalid.
    Corrupt,
    /// A bound prevented complete verification.
    Incomplete,
}

/// Verification result for one canonical persisted segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedPersistedSegment {
    /// `numeric` or `blob`.
    pub lane: String,
    /// Segment level.
    pub level: u8,
    /// Segment id.
    pub segment_id: u64,
    /// Bounded relative segment-root path.
    pub path: String,
    /// Manifest schema version when readable.
    pub manifest_version: Option<u16>,
    /// Manifest CRC32 status.
    pub manifest_checksum_valid: Option<bool>,
    /// Final verification state.
    pub status: PersistedSegmentStatus,
}

/// Complete bounded inspection report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirectoryInspectionReport {
    /// Stable report schema.
    pub report_schema_version: u16,
    /// Bounded and sanitized source path.
    pub source_path: String,
    /// Determined format identity.
    pub format: DataDirectoryFormat,
    /// Overall health.
    pub health: InspectionHealth,
    /// Parsed data-directory manifest fields.
    pub manifest: Option<InspectedDataDirectoryManifest>,
    /// Canonical series-registry snapshot state.
    pub registry: InspectedSeriesRegistry,
    /// WAL publish marker.
    pub wal_published: InspectedWalPublishedMarker,
    /// Canonical WAL segment results.
    pub wal_segments: Vec<InspectedWalSegment>,
    /// Canonical persisted segment results.
    pub segments: Vec<InspectedPersistedSegment>,
    /// Stable findings.
    pub findings: Vec<InspectionFinding>,
    /// Byte/frame ranges that cannot safely be retained by salvage.
    pub discarded_ranges: Vec<InspectionDiscardedRange>,
    /// Work performed.
    pub work: InspectionWork,
    /// Whether all requested checks and report retention completed.
    pub completeness: InspectionCompleteness,
}

/// Finite work limits for destination-only salvage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataDirectorySalvageLimits {
    /// Limits used for both source inspection and staged-result verification.
    pub inspection: DataDirectoryInspectionLimits,
    /// Maximum directories and files created in staging, including the rewritten WAL marker.
    pub max_copy_entries: u64,
    /// Maximum regular-file bytes copied or generated in staging.
    pub max_copy_bytes: u64,
    /// Explicit caller attestation that no process can mutate the source during salvage.
    pub source_is_offline_and_immutable: bool,
}

impl Default for DataDirectorySalvageLimits {
    fn default() -> Self {
        Self {
            inspection: DataDirectoryInspectionLimits::default(),
            max_copy_entries: 100_000,
            max_copy_bytes: 8 * 1024 * 1024 * 1024,
            source_is_offline_and_immutable: false,
        }
    }
}

impl DataDirectorySalvageLimits {
    fn validate(self) -> Result<Self> {
        self.inspection.validate()?;
        if self.max_copy_entries == 0 || self.max_copy_bytes == 0 {
            return Err(TsinkError::InvalidConfiguration(
                "data-directory salvage copy limits must be non-zero".to_string(),
            ));
        }
        if !self.source_is_offline_and_immutable {
            return Err(TsinkError::InvalidConfiguration(
                "data-directory salvage requires explicit source_is_offline_and_immutable=true attestation"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

/// Stable WAL high-water value retained by salvage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SalvagedWalHighwater {
    /// WAL segment id.
    pub segment: u64,
    /// WAL frame sequence.
    pub frame: u64,
}

/// How a non-WAL-data source path was handled by salvage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SalvagePathDispositionKind {
    /// The source object is intentionally absent from the destination.
    Omitted,
    /// The destination contains a newly generated replacement.
    Replaced,
}

/// Explicit evidence for an operational path omitted or replaced by salvage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SalvagePathDisposition {
    /// Bounded source-relative path.
    pub path: String,
    /// Destination treatment.
    pub disposition: SalvagePathDispositionKind,
    /// Stable reason code.
    pub reason_code: String,
}

/// Retained-output accounting for the complete salvage report, including its embedded inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SalvageReportRetention {
    /// Retained collection items across the outer and embedded reports.
    pub report_items: u64,
    /// Retained rendered-path bytes across the outer and embedded reports.
    pub retained_path_bytes: u64,
}

/// Machine-readable result of a successful destination-only salvage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirectorySalvageReport {
    /// Stable report schema.
    pub report_schema_version: u16,
    /// Bounded source path.
    pub source_path: String,
    /// Bounded published destination path.
    pub destination_path: String,
    /// Number of filesystem entries created in staging.
    pub copied_entries: u64,
    /// Number of regular-file bytes copied or generated.
    pub copied_bytes: u64,
    /// WAL boundary written into the recovered destination.
    pub retained_wal_highwater: SalvagedWalHighwater,
    /// Every known unsafe or unpublished WAL byte range excluded from the destination.
    pub discarded_ranges: Vec<InspectionDiscardedRange>,
    /// Canonical WAL files wholly omitted after the first unsafe range.
    pub omitted_wal_paths: Vec<String>,
    /// Operational source paths intentionally omitted or replaced.
    pub path_dispositions: Vec<SalvagePathDisposition>,
    /// Relative path of the recoverable success evidence atomically published with the destination.
    pub persisted_report_path: String,
    /// Complete healthy inspection of the staged bytes before atomic publication.
    pub recovered_inspection: DataDirectoryInspectionReport,
    /// Bounded retained-output accounting for this entire report.
    pub retention: SalvageReportRetention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedSalvageReport {
    report_schema_version: u16,
    source_path: String,
    destination_path: String,
    retained_wal_highwater: SalvagedWalHighwater,
    discarded_ranges: Vec<InspectionDiscardedRange>,
    omitted_wal_paths: Vec<String>,
    path_dispositions: Vec<SalvagePathDisposition>,
}

#[derive(Serialize)]
struct PersistedSalvageReportRef<'a> {
    report_schema_version: u16,
    source_path: &'a str,
    destination_path: &'a str,
    retained_wal_highwater: SalvagedWalHighwater,
    discarded_ranges: &'a [InspectionDiscardedRange],
    omitted_wal_paths: &'a [String],
    path_dispositions: &'a [SalvagePathDisposition],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    File,
    Directory,
    LinkLike,
    Other,
}

#[derive(Debug)]
struct DiscoveredEntry {
    path: PathBuf,
    relative: PathBuf,
    kind: EntryKind,
    len: u64,
    identity: FileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, Copy)]
struct EntryStat {
    kind: EntryKind,
    len: u64,
    identity: FileIdentity,
}

#[derive(Debug, Deserialize)]
struct ManifestHeaderProbe {
    magic: String,
    manifest_schema_version: u16,
    payload: ManifestPayloadVersionProbe,
}

#[derive(Debug, Deserialize)]
struct ManifestPayloadVersionProbe {
    storage_format_version: u16,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    magic: String,
    manifest_schema_version: u16,
    payload_crc32: u32,
    payload: ManifestPayload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestPayload {
    storage_format_version: u16,
    minimum_reader_storage_format_version: u16,
    creating_tsink_version: Option<String>,
    last_successfully_opened_tsink_version: Option<String>,
    format_affecting_features: Vec<String>,
    timestamp_precision: ManifestTimestampPrecision,
    chunk_point_capacity: u32,
    partition_window_timestamp_units: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ManifestTimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl ManifestTimestampPrecision {
    fn as_str(self) -> &'static str {
        match self {
            Self::Nanoseconds => "nanoseconds",
            Self::Microseconds => "microseconds",
            Self::Milliseconds => "milliseconds",
            Self::Seconds => "seconds",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SegmentFileExpectation {
    kind: u8,
    len: u64,
    hash64: u64,
}

#[derive(Debug)]
struct ParsedSegmentManifest {
    version: u16,
    segment_id: u64,
    level: u8,
    wal_highwater: WalInspectionBoundary,
    checksum_valid: bool,
    files: [SegmentFileExpectation; 4],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WalInspectionBoundary {
    segment: u64,
    frame: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentIdentity {
    lane: &'static str,
    level: u8,
    segment_id: u64,
}

#[derive(Debug, Deserialize)]
struct CatalogReference {
    lane: String,
    #[serde(default)]
    tier: Option<String>,
    level: u8,
    segment_id: u64,
}

#[derive(Debug, Deserialize)]
struct LocalCatalog {
    version: u32,
    segments: Vec<CatalogReference>,
}

#[derive(Debug, Deserialize)]
struct RemoteCatalog {
    version: u32,
    entries: Vec<CatalogReference>,
}

struct InspectionContext {
    limits: DataDirectoryInspectionLimits,
    work: InspectionWork,
    bounds_hit: BTreeSet<&'static str>,
    findings: Vec<InspectionFinding>,
    discarded_ranges: Vec<InspectionDiscardedRange>,
}

impl InspectionContext {
    fn new(limits: DataDirectoryInspectionLimits) -> Self {
        Self {
            limits,
            work: InspectionWork::default(),
            bounds_hit: BTreeSet::new(),
            findings: Vec::new(),
            discarded_ranges: Vec::new(),
        }
    }

    fn hit(&mut self, name: &'static str) {
        self.bounds_hit.insert(name);
    }

    fn reserve_report_item(&mut self) -> bool {
        if self.work.report_items >= self.limits.max_report_items {
            self.hit("max_report_items");
            return false;
        }
        self.work.report_items += 1;
        true
    }

    fn cancel_report_item(&mut self) {
        self.work.report_items = self.work.report_items.saturating_sub(1);
    }

    fn rendered_path(&mut self, path: &Path) -> Option<String> {
        let raw = path.as_os_str().as_encoded_bytes();
        let per_path = usize::try_from(self.limits.max_path_bytes).unwrap_or(usize::MAX);
        let retained_left = self
            .limits
            .max_retained_path_bytes
            .saturating_sub(self.work.retained_path_bytes);
        let retained_left = usize::try_from(retained_left).unwrap_or(usize::MAX);
        if retained_left < TRUNCATED_PATH_SUFFIX.len() {
            self.hit("max_retained_path_bytes");
            return None;
        }
        let cap = per_path.min(retained_left);
        let encoded_len = rendered_path_len(raw);
        let truncated = encoded_len > cap;
        let path_limit_hit = encoded_len > per_path;
        let retained_limit_hit = encoded_len > retained_left;
        let payload_cap = if truncated {
            cap.saturating_sub(TRUNCATED_PATH_SUFFIX.len())
        } else {
            cap
        };
        let mut rendered = String::new();
        if rendered.try_reserve(encoded_len.min(cap)).is_err() {
            self.hit("path_allocation_failed");
            return None;
        }
        for byte in raw.iter().copied() {
            let encoded_byte_len = rendered_path_byte_len(byte);
            if rendered.len().saturating_add(encoded_byte_len) > payload_cap {
                break;
            }
            push_rendered_path_byte(&mut rendered, byte);
        }
        if truncated {
            rendered.push_str(TRUNCATED_PATH_SUFFIX);
            if path_limit_hit {
                self.hit("max_path_bytes");
            }
            if retained_limit_hit {
                self.hit("max_retained_path_bytes");
            }
        }
        self.work.retained_path_bytes = self
            .work
            .retained_path_bytes
            .saturating_add(rendered.len() as u64);
        Some(rendered)
    }

    fn finding(
        &mut self,
        code: &'static str,
        severity: InspectionSeverity,
        relative: Option<&Path>,
        message: impl Into<String>,
        byte_range: Option<InspectionByteRange>,
    ) {
        if self.work.issues >= self.limits.max_issues {
            self.hit("max_issues");
            return;
        }
        if !self.reserve_report_item() {
            return;
        }
        let path = relative.and_then(|path| self.rendered_path(path));
        self.work.issues += 1;
        self.findings.push(InspectionFinding {
            code: code.to_string(),
            severity,
            path,
            message: bounded_message(message.into()),
            byte_range,
        });
    }

    fn discarded(
        &mut self,
        relative: &Path,
        bytes: InspectionByteRange,
        first_frame: Option<u64>,
        last_frame: Option<u64>,
        reason_code: &'static str,
    ) {
        if !self.reserve_report_item() {
            return;
        }
        let Some(path) = self.rendered_path(relative) else {
            self.cancel_report_item();
            return;
        };
        self.discarded_ranges.push(InspectionDiscardedRange {
            path,
            bytes,
            first_frame,
            last_frame,
            reason_code: reason_code.to_string(),
        });
    }

    fn admit_read(&mut self, bytes: u64) -> bool {
        let Some(required) = self.work.bytes_read.checked_add(bytes) else {
            self.hit("max_bytes_read");
            return false;
        };
        if required > self.limits.max_bytes_read {
            self.hit("max_bytes_read");
            return false;
        }
        self.work.bytes_read = required;
        true
    }

    fn admit_hash(&mut self, bytes: u64) -> bool {
        let Some(required) = self.work.bytes_hashed.checked_add(bytes) else {
            self.hit("max_bytes_hashed");
            return false;
        };
        if required > self.limits.max_bytes_hashed {
            self.hit("max_bytes_hashed");
            return false;
        }
        self.work.bytes_hashed = required;
        true
    }
}

struct InspectionSource<'a> {
    display: &'a Path,
    root: &'a File,
}

/// Inspects `source` without creating, renaming, truncating, quarantining, or locking anything.
///
/// On Unix, an absent source reports empty and an existing source must be a real directory reached
/// without any symlink component. Traversal and regular-file reads remain relative to a pinned
/// root handle; discovered links are reported without being followed, every intermediate
/// component is opened with no-follow semantics, and identity and length are revalidated after each
/// read. Other platforms conservatively refuse inspection until equivalent handle-relative
/// primitives are implemented.
pub fn inspect_data_directory(
    source: impl AsRef<Path>,
    limits: DataDirectoryInspectionLimits,
) -> Result<DataDirectoryInspectionReport> {
    let limits = limits.validate()?;
    let source = source.as_ref();
    let requested_root =
        match open_plain_directory_nofollow(source, "data-directory inspection root") {
            Ok(root) => root,
            Err(TsinkError::IoWithPath {
                source: source_err, ..
            }) if source_err.kind() == std::io::ErrorKind::NotFound => {
                let mut ctx = InspectionContext::new(limits);
                let source_path = ctx
                    .rendered_path(source)
                    .unwrap_or_else(|| "<source-path-unavailable>".to_string());
                return Ok(finalize_report(
                    ctx,
                    source_path,
                    DataDirectoryFormat::Empty,
                    None,
                    InspectedSeriesRegistry {
                        present: false,
                        valid: false,
                        series_count: None,
                    },
                    InspectedWalPublishedMarker {
                        present: false,
                        checksum_valid: None,
                        segment: None,
                        frame: None,
                        reset_through_segment: None,
                        reset_through_frame: None,
                        frame_type: None,
                    },
                    Vec::new(),
                    Vec::new(),
                ));
            }
            Err(err) => return Err(err),
        };
    let source_meta = requested_root
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: source.to_path_buf(),
            source: source_err,
        })?;
    if is_link_like(&source_meta) || !source_meta.file_type().is_dir() {
        let mut ctx = InspectionContext::new(limits);
        let source_path = ctx
            .rendered_path(source)
            .unwrap_or_else(|| "<source-path-unavailable>".to_string());
        return Err(TsinkError::InvalidConfiguration(format!(
            "inspection source must be a real, non-link directory: {source_path}"
        )));
    }
    inspect_data_directory_from_open_root(source, &requested_root, limits)
}

fn inspect_data_directory_from_open_root(
    source_display: &Path,
    source_root: &File,
    limits: DataDirectoryInspectionLimits,
) -> Result<DataDirectoryInspectionReport> {
    let mut ctx = InspectionContext::new(limits);
    let source_path = ctx
        .rendered_path(source_display)
        .unwrap_or_else(|| "<source-path-unavailable>".to_string());
    let source = InspectionSource {
        display: source_display,
        root: source_root,
    };
    let mut entries = Vec::new();
    walk_namespace(
        &mut ctx,
        &source,
        source.root,
        Path::new(""),
        0,
        &mut entries,
    )?;
    report_unknown_namespace(&mut ctx, &entries);
    report_unverified_recovery_objects(&mut ctx, &entries);

    let (mut format, manifest) = inspect_data_manifest(&mut ctx, &source, &entries)?;
    let registry = inspect_registry_snapshot(
        &mut ctx,
        &source,
        &entries,
        format == DataDirectoryFormat::CurrentManifest,
    )?;
    let mut wal_published = inspect_wal_published(&mut ctx, &source, &entries)?;
    let wal_segments = inspect_wal_segments(&mut ctx, &source, &entries, &mut wal_published)?;
    inspect_persisted_salvage_report(&mut ctx, &source, &entries, &wal_published, &wal_segments)?;
    let (segments, persisted_wal_highwater) =
        inspect_persisted_segments(&mut ctx, &source, &entries)?;
    validate_wal_published_boundary(
        &mut ctx,
        &wal_published,
        &wal_segments,
        persisted_wal_highwater,
    );
    validate_catalog_references(&mut ctx, &source, &entries, &segments)?;
    report_unverified_cross_artifact_state(&mut ctx, &registry, &wal_segments, &segments);

    if manifest.is_none() && !matches!(format, DataDirectoryFormat::Empty) {
        let has_canonical_wal = !wal_segments.is_empty();
        let valid_v2_identity = wal_segments
            .iter()
            .any(|segment| segment.status == WalSegmentStatus::Clean && segment.valid_frames > 0)
            || (has_canonical_wal
                && wal_published.checksum_valid == Some(true)
                && wal_segments
                    .iter()
                    .all(|segment| segment.status == WalSegmentStatus::Clean))
            || segments
                .iter()
                .any(|segment| segment.status == PersistedSegmentStatus::Clean)
            || registry.valid;
        format = if valid_v2_identity {
            DataDirectoryFormat::SupportedPreManifestV2
        } else if entries.iter().all(is_ignorable_empty_entry) {
            DataDirectoryFormat::Empty
        } else if has_canonical_wal
            || !segments.is_empty()
            || find_entry(&entries, Path::new("series_index.bin")).is_some()
            || ctx.findings.iter().any(|finding| {
                matches!(finding.severity, InspectionSeverity::Error)
                    && finding.code.starts_with("format.")
            })
        {
            DataDirectoryFormat::Corrupt
        } else {
            DataDirectoryFormat::UnknownForeign
        };
    }

    Ok(finalize_report(
        ctx,
        source_path,
        format,
        manifest,
        registry,
        wal_published,
        wal_segments,
        segments,
    ))
}

/// Creates a new destination after explicitly discarding every source WAL byte.
///
/// The deliberately narrow v1 policy requires an explicitly attested offline/immutable source,
/// a current checksum-valid manifest, an empty production-decoded registry, no persisted segments
/// or catalog/auxiliary state, and a bounded WAL corruption range. It retains an empty WAL at
/// frame zero and records full-file discard ranges for every source WAL segment. The source is
/// never opened for writing. Unix implementations copy and publish through handle-relative
/// no-follow operations; other platforms refuse salvage. The destination must be absent and
/// non-overlapping. A success report is persisted inside the atomically published destination so
/// stdout failure cannot erase the recovery evidence.
pub fn salvage_data_directory(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: DataDirectorySalvageLimits,
) -> Result<DataDirectorySalvageReport> {
    salvage_data_directory_with_before_publish(
        source.as_ref(),
        destination.as_ref(),
        limits,
        |_| Ok(()),
    )
}

fn salvage_data_directory_with_before_publish<F>(
    source: &Path,
    destination: &Path,
    limits: DataDirectorySalvageLimits,
    before_publish: F,
) -> Result<DataDirectorySalvageReport>
where
    F: FnOnce(&Path) -> Result<()>,
{
    salvage_data_directory_with_publish_hooks(source, destination, limits, before_publish, |_| {
        Ok(())
    })
}

fn salvage_data_directory_with_publish_hooks<F, G>(
    source: &Path,
    destination: &Path,
    limits: DataDirectorySalvageLimits,
    before_publish: F,
    after_final_parent_attestation: G,
) -> Result<DataDirectorySalvageReport>
where
    F: FnOnce(&Path) -> Result<()>,
    G: FnOnce(&Path) -> Result<()>,
{
    let limits = limits.validate()?;
    #[cfg(not(unix))]
    {
        let _ = (
            source,
            destination,
            before_publish,
            after_final_parent_attestation,
        );
        return Err(TsinkError::InvalidConfiguration(
            "data-directory salvage is unavailable on this platform because no handle-relative no-follow filesystem primitive is implemented"
                .to_string(),
        ));
    }
    let source_root = absolute_normalized_display_path(source, "data-directory salvage source")?;
    let source_root_handle =
        open_plain_directory_nofollow(source, "data-directory salvage source")?;
    let source_identity =
        same_file::Handle::from_file(source_root_handle.try_clone().map_err(|source_err| {
            TsinkError::IoWithPath {
                path: source_root.clone(),
                source: source_err,
            }
        })?)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: source_root.clone(),
            source: source_err,
        })?;
    let source_report = inspect_data_directory_from_open_root(
        &source_root,
        &source_root_handle,
        limits.inspection,
    )?;
    validate_salvage_source_report(&source_report)?;
    let destination_name = destination.file_name().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "salvage destination must have a final path component: {}",
            destination.display()
        ))
    })?;
    let destination_parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = absolute_normalized_display_path(
        destination_parent,
        "data-directory salvage destination parent",
    )?;
    let destination_parent_handle =
        open_plain_directory_nofollow(destination_parent, "data-directory salvage destination")?;
    let destination_parent_identity =
        same_file::Handle::from_file(destination_parent_handle.try_clone().map_err(
            |source_err| TsinkError::IoWithPath {
                path: canonical_parent.clone(),
                source: source_err,
            },
        )?)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: canonical_parent.clone(),
            source: source_err,
        })?;
    let canonical_destination = canonical_parent.join(destination_name);
    if handle_relative_entry_exists(&destination_parent_handle, destination_name)? {
        return Err(TsinkError::InvalidConfiguration(format!(
            "salvage destination already exists and will not be replaced: {}",
            destination.display()
        )));
    }
    if canonical_destination.starts_with(&source_root)
        || source_root.starts_with(&canonical_destination)
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "salvage source and destination overlap: source={}, destination={}",
            source.display(),
            destination.display()
        )));
    }

    let mut discovery_ctx = InspectionContext::new(limits.inspection);
    let mut entries = Vec::new();
    let discovery_source = InspectionSource {
        display: &source_root,
        root: &source_root_handle,
    };
    walk_namespace(
        &mut discovery_ctx,
        &discovery_source,
        discovery_source.root,
        Path::new(""),
        0,
        &mut entries,
    )?;
    if !discovery_ctx.bounds_hit.is_empty() || !discovery_ctx.findings.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "salvage source changed or exceeded namespace limits after inspection".to_string(),
        ));
    }
    validate_salvage_v1_namespace(&entries)?;
    verify_salvage_source_generation(
        &source_root,
        &source_root_handle,
        &source_identity,
        &source_report,
        &entries,
        limits.inspection,
    )?;
    let destination_path = render_required_salvage_path(&canonical_destination, limits.inspection)?;
    let plan = build_salvage_copy_plan(&source_report, &entries, &destination_path, limits)?;

    let (staging, staging_name, staging_handle) =
        create_handle_relative_salvage_staging(&destination_parent_handle, &canonical_parent)?;
    let staging_identity =
        same_file::Handle::from_file(staging_handle.try_clone().map_err(|source_err| {
            TsinkError::IoWithPath {
                path: staging.clone(),
                source: source_err,
            }
        })?)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: staging.clone(),
            source: source_err,
        })?;
    let publication_visible = std::cell::Cell::new(false);
    let staged_result = (|| -> Result<DataDirectorySalvageReport> {
        execute_salvage_copy_plan(
            &source_root_handle,
            &staging_handle,
            &source_root,
            &staging,
            &entries,
            &plan,
        )?;
        verify_salvage_staging_generation(
            &canonical_parent,
            &destination_parent_identity,
            &staging,
            &staging_identity,
        )?;
        let mut recovered =
            inspect_data_directory_from_open_root(&staging, &staging_handle, limits.inspection)?;
        if recovered.health != InspectionHealth::Clean
            || !recovered.completeness.complete
            || recovered.format != DataDirectoryFormat::CurrentManifest
            || !recovered.discarded_ranges.is_empty()
        {
            return Err(TsinkError::DataCorruption(format!(
                "staged salvage did not pass a complete clean inspection with no discarded bytes: health={:?}, format={:?}, discarded_ranges={}",
                recovered.health,
                recovered.format,
                recovered.discarded_ranges.len()
            )));
        }
        let recovered_entries =
            discover_complete_namespace_generation(&staging, &staging_handle, limits.inspection)?;
        before_publish(&staging)?;
        verify_salvage_source_generation(
            &source_root,
            &source_root_handle,
            &source_identity,
            &source_report,
            &entries,
            limits.inspection,
        )?;
        verify_salvage_staging_generation(
            &canonical_parent,
            &destination_parent_identity,
            &staging,
            &staging_identity,
        )?;
        let final_recovered =
            inspect_data_directory_from_open_root(&staging, &staging_handle, limits.inspection)?;
        let final_entries =
            discover_complete_namespace_generation(&staging, &staging_handle, limits.inspection)?;
        if final_recovered != recovered
            || !namespace_signatures_match(&recovered_entries, &final_entries)
        {
            return Err(TsinkError::DataCorruption(
                "salvage staging content generation changed after verification".to_string(),
            ));
        }
        verify_salvage_staging_bytes(
            &source_root_handle,
            &staging_handle,
            &source_root,
            &staging,
            &entries,
            &final_entries,
            &plan,
        )?;
        sync_salvage_namespace(&staging_handle, &staging, &final_entries)?;
        let post_sync_entries =
            discover_complete_namespace_generation(&staging, &staging_handle, limits.inspection)?;
        if !namespace_signatures_match(&final_entries, &post_sync_entries) {
            return Err(TsinkError::DataCorruption(
                "salvage staging namespace changed during final durability synchronization"
                    .to_string(),
            ));
        }
        verify_salvage_staging_bytes(
            &source_root_handle,
            &staging_handle,
            &source_root,
            &staging,
            &entries,
            &post_sync_entries,
            &plan,
        )?;
        retarget_recovered_inspection_source(&mut recovered, &destination_path, limits.inspection)?;
        let mut retention = plan.retention;
        retention.merge_embedded_inspection(&recovered)?;
        let report = DataDirectorySalvageReport {
            report_schema_version: DATA_DIRECTORY_INSPECTION_REPORT_SCHEMA_VERSION,
            source_path: source_report.source_path.clone(),
            destination_path,
            copied_entries: plan.copy_entries,
            copied_bytes: plan.copy_bytes,
            retained_wal_highwater: plan.retained_highwater,
            discarded_ranges: plan.discarded_ranges.clone(),
            omitted_wal_paths: plan.omitted_wal_paths.clone(),
            path_dispositions: plan.path_dispositions.clone(),
            persisted_report_path: SALVAGE_REPORT_FILE_NAME.to_string(),
            recovered_inspection: recovered,
            retention: retention.snapshot(),
        };
        // The exact-byte comparison and durability pass above can be arbitrarily long. Rebind the
        // requested parent and staging identities immediately before the atomic boundary rather
        // than relying on the earlier check.
        verify_salvage_staging_generation(
            &canonical_parent,
            &destination_parent_identity,
            &staging,
            &staging_identity,
        )?;
        // This test seam deliberately sits after the last pathname attestation. Production calls
        // install a no-op; the regression proves that the unavoidable same-UID race is detected
        // after publication instead of being reported as success.
        after_final_parent_attestation(&staging)?;
        match rename_handle_relative_noreplace(
            &destination_parent_handle,
            &staging_name,
            destination_name,
        ) {
            Ok(()) => publication_visible.set(true),
            Err(publication) => {
                publication_visible.set(publication.published);
                return Err(publication.error);
            }
        }
        if !handle_relative_directory_identity_matches(
            &destination_parent_handle,
            destination_name,
            &staging_identity,
        )? || !componentwise_directory_identity_matches(
            &canonical_parent,
            &destination_parent_identity,
        )? || !componentwise_directory_identity_matches(
            &canonical_destination,
            &staging_identity,
        )? {
            return Err(TsinkError::DataCorruption(format!(
                "salvage publication crossed the atomic rename but the requested parent or destination no longer resolves to the retained identities: {}",
                canonical_destination.display()
            )));
        }
        let published_entries = discover_complete_namespace_generation(
            &canonical_destination,
            &staging_handle,
            limits.inspection,
        )?;
        if !namespace_signatures_match(&post_sync_entries, &published_entries) {
            return Err(TsinkError::DataCorruption(format!(
                "salvage publication crossed the atomic rename but its namespace generation changed at the publication boundary: {}",
                canonical_destination.display()
            )));
        }
        verify_salvage_staging_bytes(
            &source_root_handle,
            &staging_handle,
            &source_root,
            &canonical_destination,
            &entries,
            &published_entries,
            &plan,
        )?;
        Ok(report)
    })();
    match staged_result {
        Ok(report) => Ok(report),
        Err(primary) => {
            if publication_visible.get() {
                let retained = match componentwise_directory_identity_matches(
                    &canonical_destination,
                    &staging_identity,
                ) {
                    Ok(true) => format!("{}", canonical_destination.display()),
                    Ok(false) | Err(_) => format!(
                        "destination component {} beneath the retained parent originally opened as {} (the requested path no longer resolves to that parent)",
                        destination_name.to_string_lossy(),
                        canonical_parent.display()
                    ),
                };
                return Err(TsinkError::Other(format!(
                    "salvage failed after the staging tree became published: {primary}; the published tree is retained at {retained}"
                )));
            }
            let retained =
                match componentwise_directory_identity_matches(&staging, &staging_identity) {
                    Ok(true) => staging.clone(),
                    Ok(false) => match componentwise_directory_identity_matches(
                        &canonical_destination,
                        &staging_identity,
                    ) {
                        Ok(true) => canonical_destination.clone(),
                        Ok(false) | Err(_) => staging.clone(),
                    },
                    Err(_) => staging.clone(),
                };
            Err(TsinkError::Other(format!(
                "salvage failed: {primary}; populated salvage trees are never recursively deleted on error; inspect retained path: {}",
                retained.display()
            )))
        }
    }
}

fn retarget_recovered_inspection_source(
    recovered: &mut DataDirectoryInspectionReport,
    destination_path: &str,
    limits: DataDirectoryInspectionLimits,
) -> Result<()> {
    let prior = u64::try_from(recovered.source_path.len()).unwrap_or(u64::MAX);
    let replacement = u64::try_from(destination_path.len()).unwrap_or(u64::MAX);
    if replacement > u64::from(limits.max_path_bytes) {
        return Err(TsinkError::InvalidConfiguration(
            "recovered inspection destination exceeds max_path_bytes".to_string(),
        ));
    }
    recovered.work.retained_path_bytes = recovered
        .work
        .retained_path_bytes
        .checked_sub(prior)
        .and_then(|bytes| bytes.checked_add(replacement))
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    if recovered.work.retained_path_bytes > limits.max_retained_path_bytes {
        return Err(TsinkError::InvalidConfiguration(
            "recovered inspection destination exceeds max_retained_path_bytes".to_string(),
        ));
    }
    recovered.source_path = destination_path.to_string();
    Ok(())
}

#[derive(Debug)]
struct SalvageCopyPlan {
    cut_segment: u64,
    cut_path: PathBuf,
    cut_len: u64,
    retained_highwater: SalvagedWalHighwater,
    copy_entries: u64,
    copy_bytes: u64,
    discarded_ranges: Vec<InspectionDiscardedRange>,
    omitted_wal_paths: Vec<String>,
    path_dispositions: Vec<SalvagePathDisposition>,
    persisted_report_bytes: Vec<u8>,
    retention: SalvageRetentionLedger,
}

#[derive(Debug, Clone, Copy)]
struct SalvageRetentionLedger {
    report_items: u64,
    retained_path_bytes: u64,
    limits: DataDirectoryInspectionLimits,
}

impl SalvageRetentionLedger {
    fn new(limits: DataDirectoryInspectionLimits) -> Self {
        Self {
            report_items: 0,
            retained_path_bytes: 0,
            limits,
        }
    }

    fn admit_item(&mut self) -> Result<()> {
        self.report_items = self
            .report_items
            .checked_add(1)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        if self.report_items > self.limits.max_report_items {
            return Err(TsinkError::InvalidConfiguration(
                "salvage report exceeds max_report_items".to_string(),
            ));
        }
        Ok(())
    }

    fn admit_path(&mut self, path: &str) -> Result<()> {
        let path_bytes = u64::try_from(path.len()).unwrap_or(u64::MAX);
        if path_bytes > u64::from(self.limits.max_path_bytes) {
            return Err(TsinkError::InvalidConfiguration(
                "salvage report path exceeds max_path_bytes".to_string(),
            ));
        }
        self.retained_path_bytes = self
            .retained_path_bytes
            .checked_add(path_bytes)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        if self.retained_path_bytes > self.limits.max_retained_path_bytes {
            return Err(TsinkError::InvalidConfiguration(
                "salvage report exceeds max_retained_path_bytes".to_string(),
            ));
        }
        Ok(())
    }

    fn admit_item_path(&mut self, path: &str) -> Result<()> {
        self.admit_item()?;
        self.admit_path(path)
    }

    fn merge_embedded_inspection(
        &mut self,
        recovered: &DataDirectoryInspectionReport,
    ) -> Result<()> {
        self.admit_item()?;
        self.report_items = self
            .report_items
            .checked_add(recovered.work.report_items)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        self.retained_path_bytes = self
            .retained_path_bytes
            .checked_add(recovered.work.retained_path_bytes)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        if self.report_items > self.limits.max_report_items {
            return Err(TsinkError::InvalidConfiguration(
                "salvage report plus embedded inspection exceeds max_report_items".to_string(),
            ));
        }
        if self.retained_path_bytes > self.limits.max_retained_path_bytes {
            return Err(TsinkError::InvalidConfiguration(
                "salvage report plus embedded inspection exceeds max_retained_path_bytes"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn snapshot(self) -> SalvageReportRetention {
        SalvageReportRetention {
            report_items: self.report_items,
            retained_path_bytes: self.retained_path_bytes,
        }
    }
}

fn validate_salvage_source_report(report: &DataDirectoryInspectionReport) -> Result<()> {
    if !report.completeness.complete {
        return Err(TsinkError::InvalidConfiguration(
            "salvage refuses an incomplete inspection report".to_string(),
        ));
    }
    if report.format != DataDirectoryFormat::CurrentManifest
        || report
            .manifest
            .as_ref()
            .and_then(|manifest| manifest.checksum_valid)
            != Some(true)
    {
        return Err(TsinkError::InvalidConfiguration(
            "salvage currently requires a checksum-valid current data-directory manifest"
                .to_string(),
        ));
    }
    if !report.segments.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "salvage v1 refuses every data directory with persisted segments".to_string(),
        ));
    }
    if !report.registry.present || !report.registry.valid || report.registry.series_count != Some(0)
    {
        return Err(TsinkError::InvalidConfiguration(
            "salvage v1 requires a strictly decoded empty series-registry snapshot".to_string(),
        ));
    }
    if report.wal_published.checksum_valid != Some(true)
        || report.wal_published.segment.is_none()
        || report.wal_published.frame.is_none()
    {
        return Err(TsinkError::InvalidConfiguration(
            "salvage requires a valid WAL publication marker".to_string(),
        ));
    }
    let allowed = |code: &str| {
        matches!(
            code,
            "wal.corrupt_tail_header"
                | "wal.corrupt_tail_payload"
                | "wal.mid_log_magic_mismatch"
                | "wal.mid_log_payload_length_invalid"
                | "wal.mid_log_reserved_header_invalid"
                | "wal.mid_log_sequence_invalid"
                | "wal.mid_log_frame_type_invalid"
                | "wal.mid_log_checksum_mismatch"
                | "wal.mid_log_payload_invalid"
                | "wal.mid_log_truncation_before_later_segment"
                | "wal.published_boundary_missing"
                | "wal.production_replay_semantics_unverified"
        )
    };
    if report
        .findings
        .iter()
        .any(|finding| !allowed(&finding.code))
    {
        return Err(TsinkError::InvalidConfiguration(
            "salvage refuses non-WAL, namespace, catalog, manifest, or unsupported findings"
                .to_string(),
        ));
    }
    if !report.discarded_ranges.iter().any(|range| {
        matches!(
            range.reason_code.as_str(),
            "wal.corrupt_tail" | "wal.mid_log_corruption"
        )
    }) {
        return Err(TsinkError::InvalidConfiguration(
            "salvage requires a verified WAL corruption boundary; healthy copy is not salvage"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_salvage_v1_namespace(entries: &[DiscoveredEntry]) -> Result<()> {
    for entry in entries {
        let Some(components) = utf8_path_components(&entry.relative) else {
            return Err(TsinkError::InvalidConfiguration(
                "salvage v1 refuses opaque source path components".to_string(),
            ));
        };
        let allowed = match components.as_slice() {
            [DATA_DIRECTORY_MANIFEST_FILE_NAME | "series_index.bin" | ".tsink.lock"] => {
                entry.kind == EntryKind::File
            }
            ["lane_numeric" | "lane_blob" | "wal"] => entry.kind == EntryKind::Directory,
            ["lane_numeric" | "lane_blob", "segments"] => entry.kind == EntryKind::Directory,
            ["lane_numeric" | "lane_blob", "segments", "L0" | "L1" | "L2"] => {
                entry.kind == EntryKind::Directory
            }
            ["wal", name] => {
                entry.kind == EntryKind::File
                    && (*name == "wal.published"
                        || *name == "wal.log"
                        || parse_exact_hex_file(name, "wal-", ".log").is_some())
            }
            _ => false,
        };
        if !allowed {
            return Err(TsinkError::InvalidConfiguration(format!(
                "salvage v1 refuses catalog, persisted, tombstone, recovery, server, or other auxiliary path: {}",
                entry.relative.display()
            )));
        }
    }
    Ok(())
}

fn verify_salvage_source_generation(
    source: &Path,
    source_root: &File,
    source_identity: &same_file::Handle,
    expected_report: &DataDirectoryInspectionReport,
    expected_entries: &[DiscoveredEntry],
    limits: DataDirectoryInspectionLimits,
) -> Result<()> {
    if !componentwise_directory_identity_matches(source, source_identity)? {
        return Err(TsinkError::DataCorruption(
            "salvage source root identity changed during the operation".to_string(),
        ));
    }
    let observed_report = inspect_data_directory_from_open_root(source, source_root, limits)?;
    if &observed_report != expected_report {
        return Err(TsinkError::DataCorruption(
            "salvage source inspection generation changed during the operation".to_string(),
        ));
    }
    let mut ctx = InspectionContext::new(limits);
    let mut observed_entries = Vec::new();
    let inspection_source = InspectionSource {
        display: source,
        root: source_root,
    };
    walk_namespace(
        &mut ctx,
        &inspection_source,
        inspection_source.root,
        Path::new(""),
        0,
        &mut observed_entries,
    )?;
    if !ctx.bounds_hit.is_empty()
        || !ctx.findings.is_empty()
        || !namespace_signatures_match(expected_entries, &observed_entries)
    {
        return Err(TsinkError::DataCorruption(
            "salvage source namespace paths, kinds, or lengths changed during the operation"
                .to_string(),
        ));
    }
    Ok(())
}

fn verify_salvage_staging_generation(
    parent: &Path,
    parent_identity: &same_file::Handle,
    staging: &Path,
    staging_identity: &same_file::Handle,
) -> Result<()> {
    if !componentwise_directory_identity_matches(parent, parent_identity)?
        || !componentwise_directory_identity_matches(staging, staging_identity)?
    {
        return Err(TsinkError::DataCorruption(
            "salvage destination parent or staging identity changed during the operation"
                .to_string(),
        ));
    }
    Ok(())
}

fn componentwise_directory_identity_matches(
    path: &Path,
    expected: &same_file::Handle,
) -> Result<bool> {
    let current = match open_plain_directory_nofollow(path, "directory identity verification") {
        Ok(file) => file,
        Err(TsinkError::IoWithPath { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(false);
        }
        Err(err) => return Err(err),
    };
    let current =
        same_file::Handle::from_file(current).map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    Ok(&current == expected)
}

fn discover_complete_namespace_generation(
    root: &Path,
    root_handle: &File,
    limits: DataDirectoryInspectionLimits,
) -> Result<Vec<DiscoveredEntry>> {
    let mut ctx = InspectionContext::new(limits);
    let mut entries = Vec::new();
    let source = InspectionSource {
        display: root,
        root: root_handle,
    };
    walk_namespace(
        &mut ctx,
        &source,
        source.root,
        Path::new(""),
        0,
        &mut entries,
    )?;
    if !ctx.bounds_hit.is_empty() || !ctx.findings.is_empty() {
        return Err(TsinkError::DataCorruption(
            "salvage staging namespace could not be completely revalidated".to_string(),
        ));
    }
    Ok(entries)
}

fn namespace_signatures_match(expected: &[DiscoveredEntry], observed: &[DiscoveredEntry]) -> bool {
    expected.len() == observed.len()
        && expected.iter().zip(observed).all(|(left, right)| {
            left.relative.as_os_str().as_encoded_bytes()
                == right.relative.as_os_str().as_encoded_bytes()
                && left.kind == right.kind
                && left.len == right.len
                && left.identity == right.identity
        })
}

fn build_salvage_copy_plan(
    report: &DataDirectoryInspectionReport,
    entries: &[DiscoveredEntry],
    destination_path: &str,
    limits: DataDirectorySalvageLimits,
) -> Result<SalvageCopyPlan> {
    if !report.discarded_ranges.iter().any(|range| {
        range.reason_code == "wal.corrupt_tail" || range.reason_code == "wal.mid_log_corruption"
    }) {
        return Err(TsinkError::InvalidConfiguration(
            "salvage report has no canonical WAL corruption range".to_string(),
        ));
    }
    let first_segment = report.wal_segments.first().ok_or_else(|| {
        TsinkError::InvalidConfiguration(
            "salvage v1 requires at least one canonical WAL segment".to_string(),
        )
    })?;
    let cut_segment = first_segment.segment_id;
    let cut_len = 0;
    let cut_path = PathBuf::from(&first_segment.path);
    let cut_entry = find_entry(entries, &cut_path).ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "salvage cut path disappeared after inspection: {}",
            cut_path.display()
        ))
    })?;
    if cut_entry.kind != EntryKind::File {
        return Err(TsinkError::DataCorruption(format!(
            "salvage reset path is not a regular WAL file: {}",
            cut_path.display()
        )));
    }
    let retained_highwater = SalvagedWalHighwater {
        segment: cut_segment,
        frame: 0,
    };

    let mut retention = SalvageRetentionLedger::new(limits.inspection);
    retention.admit_path(&report.source_path)?;
    retention.admit_path(destination_path)?;
    retention.admit_item()?; // retained_wal_highwater
    let mut omitted_wal_paths = Vec::new();
    let mut discarded_ranges = Vec::new();
    discarded_ranges
        .try_reserve(report.discarded_ranges.len())
        .map_err(|err| {
            TsinkError::Other(format!(
                "failed reserving bounded salvage discard evidence: {err}"
            ))
        })?;
    for range in &report.discarded_ranges {
        retention.admit_item_path(&range.path)?;
        discarded_ranges.push(range.clone());
    }
    for segment in &report.wal_segments {
        if segment.segment_id > cut_segment {
            retention.admit_item_path(&segment.path)?;
            omitted_wal_paths.push(segment.path.clone());
        }
        if !discarded_ranges.iter().any(|range| {
            range.path == segment.path
                && range.bytes.start == 0
                && range.bytes.end == segment.file_len
                && range.reason_code == "wal.salvage_v1_full_reset"
        }) {
            let first_frame = std::iter::once(segment.first_valid_frame)
                .chain(
                    report
                        .discarded_ranges
                        .iter()
                        .filter(|range| range.path == segment.path)
                        .flat_map(|range| [range.first_frame, range.last_frame]),
                )
                .flatten()
                .min();
            let unknown_final_frame = report
                .discarded_ranges
                .iter()
                .filter(|range| range.path == segment.path)
                .any(|range| range.bytes.end == segment.file_len && range.last_frame.is_none());
            let last_frame = (!unknown_final_frame)
                .then(|| {
                    std::iter::once(segment.last_valid_frame)
                        .chain(
                            report
                                .discarded_ranges
                                .iter()
                                .filter(|range| range.path == segment.path)
                                .flat_map(|range| [range.first_frame, range.last_frame]),
                        )
                        .flatten()
                        .max()
                })
                .flatten();
            retention.admit_item_path(&segment.path)?;
            discarded_ranges.push(InspectionDiscardedRange {
                path: segment.path.clone(),
                bytes: InspectionByteRange {
                    start: 0,
                    end: segment.file_len,
                },
                first_frame,
                last_frame,
                reason_code: "wal.salvage_v1_full_reset".to_string(),
            });
        }
    }
    discarded_ranges.sort_by(|left, right| {
        (
            &left.path,
            left.bytes.start,
            left.bytes.end,
            &left.reason_code,
        )
            .cmp(&(
                &right.path,
                right.bytes.start,
                right.bytes.end,
                &right.reason_code,
            ))
    });

    let mut copy_entries = 1u64; // the exclusively created staging root
    let mut copy_bytes = WAL_PUBLISHED_V2_BYTES;
    for entry in entries {
        if should_omit_salvage_entry(&entry.relative, cut_segment) {
            continue;
        }
        copy_entries = copy_entries
            .checked_add(1)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        if entry.kind == EntryKind::File {
            let bytes = if entry.relative == cut_path {
                cut_len
            } else {
                entry.len
            };
            copy_bytes = copy_bytes
                .checked_add(bytes)
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        }
    }
    copy_entries = copy_entries
        .checked_add(1)
        .ok_or(TsinkError::WriteBatchSizeOverflow)?; // rewritten wal.published
    let mut path_dispositions = Vec::new();
    if find_entry(entries, Path::new(".tsink.lock")).is_some() {
        retention.admit_item_path(".tsink.lock")?;
        path_dispositions.push(SalvagePathDisposition {
            path: ".tsink.lock".to_string(),
            disposition: SalvagePathDispositionKind::Omitted,
            reason_code: "operational.lock_not_copied".to_string(),
        });
    }
    if find_entry(entries, Path::new("wal/wal.published")).is_some() {
        retention.admit_item_path("wal/wal.published")?;
        path_dispositions.push(SalvagePathDisposition {
            path: "wal/wal.published".to_string(),
            disposition: SalvagePathDispositionKind::Replaced,
            reason_code: "wal.published_rewritten_to_retained_highwater".to_string(),
        });
    }
    retention.admit_item_path(SALVAGE_REPORT_FILE_NAME)?;
    let persisted_report_bytes = serde_json::to_vec_pretty(&PersistedSalvageReportRef {
        report_schema_version: DATA_DIRECTORY_INSPECTION_REPORT_SCHEMA_VERSION,
        source_path: &report.source_path,
        destination_path,
        retained_wal_highwater: retained_highwater,
        discarded_ranges: &discarded_ranges,
        omitted_wal_paths: &omitted_wal_paths,
        path_dispositions: &path_dispositions,
    })
    .map_err(|err| {
        TsinkError::Other(format!(
            "failed encoding bounded persisted salvage report: {err}"
        ))
    })?;
    if persisted_report_bytes.len() as u64 > limits.inspection.max_catalog_bytes {
        return Err(TsinkError::InvalidConfiguration(format!(
            "persisted salvage report requires {} bytes, max_catalog_bytes is {}",
            persisted_report_bytes.len(),
            limits.inspection.max_catalog_bytes
        )));
    }
    copy_entries = copy_entries
        .checked_add(1)
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    copy_bytes = copy_bytes
        .checked_add(u64::try_from(persisted_report_bytes.len()).unwrap_or(u64::MAX))
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    if copy_entries > limits.max_copy_entries {
        return Err(TsinkError::InvalidConfiguration(format!(
            "salvage copy requires {copy_entries} entries, limit is {}",
            limits.max_copy_entries
        )));
    }
    if copy_bytes > limits.max_copy_bytes {
        return Err(TsinkError::InvalidConfiguration(format!(
            "salvage copy requires {copy_bytes} bytes, limit is {}",
            limits.max_copy_bytes
        )));
    }
    Ok(SalvageCopyPlan {
        cut_segment,
        cut_path,
        cut_len,
        retained_highwater,
        copy_entries,
        copy_bytes,
        discarded_ranges,
        omitted_wal_paths,
        path_dispositions,
        persisted_report_bytes,
        retention,
    })
}

fn execute_salvage_copy_plan(
    source_root: &File,
    staging_root: &File,
    source_display: &Path,
    staging_display: &Path,
    entries: &[DiscoveredEntry],
    plan: &SalvageCopyPlan,
) -> Result<()> {
    let mut directories = entries
        .iter()
        .filter(|entry| {
            entry.kind == EntryKind::Directory
                && !should_omit_salvage_entry(&entry.relative, plan.cut_segment)
        })
        .collect::<Vec<_>>();
    directories.sort_by(|left, right| {
        let left_depth = left.relative.components().count();
        let right_depth = right.relative.components().count();
        (left_depth, left.relative.as_os_str().as_encoded_bytes())
            .cmp(&(right_depth, right.relative.as_os_str().as_encoded_bytes()))
    });
    for entry in directories {
        create_relative_directory_nofollow(
            staging_root,
            &entry.relative,
            &staging_display.join(&entry.relative),
        )?;
    }

    let mut files = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .collect::<Vec<_>>();
    files.sort_by(|left, right| {
        left.relative
            .as_os_str()
            .as_encoded_bytes()
            .cmp(right.relative.as_os_str().as_encoded_bytes())
    });
    for entry in files {
        if should_omit_salvage_entry(&entry.relative, plan.cut_segment) {
            continue;
        }
        let copy_len = if entry.relative == plan.cut_path {
            plan.cut_len
        } else {
            entry.len
        };
        copy_relative_regular_file_nofollow(
            source_root,
            staging_root,
            source_display,
            staging_display,
            entry,
            copy_len,
        )?;
    }
    let marker_relative = Path::new("wal/wal.published");
    let marker_path = staging_display.join(marker_relative);
    let marker = encode_salvaged_wal_marker(plan.retained_highwater);
    let (mut marker_file, marker_parent) =
        create_relative_regular_file_nofollow(staging_root, marker_relative, &marker_path, 0o600)?;
    marker_file
        .write_all(&marker)
        .and_then(|_| marker_file.flush())
        .map_err(|source_err| TsinkError::IoWithPath {
            path: marker_path.clone(),
            source: source_err,
        })?;
    marker_file
        .sync_all()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: marker_path.clone(),
            source: source_err,
        })?;
    marker_parent
        .sync_all()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: marker_path.clone(),
            source: source_err,
        })?;
    let report_relative = Path::new(SALVAGE_REPORT_FILE_NAME);
    let report_path = staging_display.join(report_relative);
    let (mut report_file, report_parent) =
        create_relative_regular_file_nofollow(staging_root, report_relative, &report_path, 0o600)?;
    report_file
        .write_all(&plan.persisted_report_bytes)
        .and_then(|_| report_file.flush())
        .and_then(|_| report_file.sync_all())
        .and_then(|_| report_parent.sync_all())
        .map_err(|source_err| TsinkError::IoWithPath {
            path: report_path,
            source: source_err,
        })?;

    let mut directories = entries
        .iter()
        .filter(|entry| {
            entry.kind == EntryKind::Directory
                && !should_omit_salvage_entry(&entry.relative, plan.cut_segment)
        })
        .map(|entry| (&entry.relative, staging_display.join(&entry.relative)))
        .collect::<Vec<_>>();
    directories.sort_by_key(|(relative, _)| std::cmp::Reverse(relative.components().count()));
    for (relative, display) in directories {
        open_relative_directory_nofollow(staging_root, relative, &display)?
            .sync_all()
            .map_err(|source_err| TsinkError::IoWithPath {
                path: display,
                source: source_err,
            })?;
    }
    staging_root
        .sync_all()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: staging_display.to_path_buf(),
            source: source_err,
        })
}

fn verify_salvage_staging_bytes(
    source_root: &File,
    staging_root: &File,
    source_display: &Path,
    staging_display: &Path,
    source_entries: &[DiscoveredEntry],
    staging_entries: &[DiscoveredEntry],
    plan: &SalvageCopyPlan,
) -> Result<()> {
    let source = InspectionSource {
        display: source_display,
        root: source_root,
    };
    let staging = InspectionSource {
        display: staging_display,
        root: staging_root,
    };
    for source_entry in source_entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
    {
        if should_omit_salvage_entry(&source_entry.relative, plan.cut_segment) {
            continue;
        }
        let staging_entry =
            find_entry(staging_entries, &source_entry.relative).ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "salvage staging is missing expected file {}",
                    source_entry.relative.display()
                ))
            })?;
        if source_entry.relative == plan.cut_path {
            verify_regular_file_matches_bytes(&staging, staging_entry, &[])?;
        } else {
            compare_regular_files_exact(&source, source_entry, &staging, staging_entry)?;
        }
    }

    let marker_entry =
        find_entry(staging_entries, Path::new("wal/wal.published")).ok_or_else(|| {
            TsinkError::DataCorruption("salvage staging marker is missing".to_string())
        })?;
    verify_regular_file_matches_bytes(
        &staging,
        marker_entry,
        &encode_salvaged_wal_marker(plan.retained_highwater),
    )?;
    let report_entry = find_entry(staging_entries, Path::new(SALVAGE_REPORT_FILE_NAME))
        .ok_or_else(|| {
            TsinkError::DataCorruption("persisted salvage evidence is missing".to_string())
        })?;
    verify_regular_file_matches_bytes(&staging, report_entry, &plan.persisted_report_bytes)
}

fn compare_regular_files_exact(
    expected_source: &InspectionSource<'_>,
    expected_entry: &DiscoveredEntry,
    observed_source: &InspectionSource<'_>,
    observed_entry: &DiscoveredEntry,
) -> Result<()> {
    if expected_entry.len != observed_entry.len {
        return Err(TsinkError::DataCorruption(format!(
            "salvage staging file length differs from its source: {}",
            observed_entry.path.display()
        )));
    }
    let mut expected = open_regular_nofollow(expected_source, expected_entry)?;
    let mut observed = open_regular_nofollow(observed_source, observed_entry)?;
    let mut expected_buffer = [0u8; HASH_BUFFER_BYTES];
    let mut observed_buffer = [0u8; HASH_BUFFER_BYTES];
    let mut remaining = expected_entry.len;
    while remaining > 0 {
        let take =
            usize::try_from(remaining.min(HASH_BUFFER_BYTES as u64)).unwrap_or(HASH_BUFFER_BYTES);
        expected
            .read_exact(&mut expected_buffer[..take])
            .map_err(|source_err| TsinkError::IoWithPath {
                path: expected_entry.path.clone(),
                source: source_err,
            })?;
        observed
            .read_exact(&mut observed_buffer[..take])
            .map_err(|source_err| TsinkError::IoWithPath {
                path: observed_entry.path.clone(),
                source: source_err,
            })?;
        if expected_buffer[..take] != observed_buffer[..take] {
            return Err(TsinkError::DataCorruption(format!(
                "salvage staging file bytes differ from their source: {}",
                observed_entry.path.display()
            )));
        }
        remaining -= take as u64;
    }
    verify_eof_after_exact_read(&mut expected, expected_entry)?;
    verify_eof_after_exact_read(&mut observed, observed_entry)?;
    verify_file_identity_after_read(&expected, expected_source, expected_entry)?;
    verify_file_identity_after_read(&observed, observed_source, observed_entry)
}

fn verify_regular_file_matches_bytes(
    source: &InspectionSource<'_>,
    entry: &DiscoveredEntry,
    expected: &[u8],
) -> Result<()> {
    if entry.len != u64::try_from(expected.len()).unwrap_or(u64::MAX) {
        return Err(TsinkError::DataCorruption(format!(
            "salvage-generated file has an unexpected length: {}",
            entry.path.display()
        )));
    }
    let mut file = open_regular_nofollow(source, entry)?;
    let mut offset = 0usize;
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    while offset < expected.len() {
        let take = (expected.len() - offset).min(HASH_BUFFER_BYTES);
        file.read_exact(&mut buffer[..take])
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry.path.clone(),
                source: source_err,
            })?;
        if buffer[..take] != expected[offset..offset + take] {
            return Err(TsinkError::DataCorruption(format!(
                "salvage-generated file bytes changed before publication: {}",
                entry.path.display()
            )));
        }
        offset += take;
    }
    verify_eof_after_exact_read(&mut file, entry)?;
    verify_file_identity_after_read(&file, source, entry)
}

fn sync_salvage_namespace(
    staging_root: &File,
    staging_display: &Path,
    entries: &[DiscoveredEntry],
) -> Result<()> {
    let staging = InspectionSource {
        display: staging_display,
        root: staging_root,
    };
    for entry in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
        let file = open_regular_nofollow(&staging, entry)?;
        file.sync_all()
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry.path.clone(),
                source: source_err,
            })?;
    }
    let mut directories = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Directory)
        .collect::<Vec<_>>();
    directories.sort_by_key(|entry| std::cmp::Reverse(entry.relative.components().count()));
    for entry in directories {
        open_relative_directory_nofollow(staging_root, &entry.relative, &entry.path)?
            .sync_all()
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry.path.clone(),
                source: source_err,
            })?;
    }
    staging_root
        .sync_all()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: staging_display.to_path_buf(),
            source: source_err,
        })
}

fn should_omit_salvage_entry(relative: &Path, cut_segment: u64) -> bool {
    relative == Path::new(".tsink.lock")
        || relative == Path::new("wal/wal.published")
        || relative == Path::new("wal/wal.published.tmp")
        || wal_segment_id_from_relative(relative).is_some_and(|segment_id| segment_id > cut_segment)
}

fn absolute_normalized_display_path(path: &Path, operation: &str) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(TsinkError::Io)?.join(path)
    };
    if absolute.components().any(|component| {
        matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::Prefix(_)
        )
    }) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "{operation} must use a normalized path without parent traversal: {}",
            path.display()
        )));
    }
    Ok(absolute)
}

#[cfg(unix)]
fn open_plain_directory_nofollow(path: &Path, operation: &str) -> Result<File> {
    let absolute = path.is_absolute();
    let anchor_path = if absolute {
        Path::new("/")
    } else {
        Path::new(".")
    };
    let mut current = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(anchor_path)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: anchor_path.to_path_buf(),
            source: source_err,
        })?;
    let mut traversed = if absolute {
        PathBuf::from("/")
    } else {
        PathBuf::from(".")
    };
    for component in path.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => {
                traversed.push(name);
                current = open_directory_component_nofollow(&current, name, &traversed)?;
            }
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "{operation} must use a normalized path without parent traversal: {}",
                    path.display()
                )))
            }
        }
    }
    let metadata = current
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source: source_err,
        })?;
    if !metadata.file_type().is_dir() {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} is not a directory: {}",
            path.display()
        )));
    }
    Ok(current)
}

#[cfg(not(unix))]
fn open_plain_directory_nofollow(_path: &Path, _operation: &str) -> Result<File> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative data-directory access is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn c_name(name: &OsStr, display: &Path) -> Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| {
        TsinkError::InvalidConfiguration(format!(
            "filesystem component contains an interior NUL: {}",
            display.display()
        ))
    })
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
struct DirectoryStream(*mut libc::DIR);

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
impl Drop for DirectoryStream {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn set_thread_errno(value: libc::c_int) {
    unsafe {
        *libc::__errno_location() = value;
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn set_thread_errno(value: libc::c_int) {
    unsafe {
        *libc::__error() = value;
    }
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn read_directory_names_nofollow(
    directory: &File,
    display: &Path,
    relative: &Path,
    source_display: &Path,
    max_entries: u64,
    max_retained_bytes: u64,
) -> Result<(Vec<OsString>, bool, bool, u64)> {
    let duplicate = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let raw = unsafe { libc::fdopendir(duplicate) };
    if raw.is_null() {
        let source_err = std::io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: source_err,
        });
    }
    unsafe {
        libc::rewinddir(raw);
    }
    let stream = DirectoryStream(raw);
    let mut names = Vec::new();
    let mut retained_bytes = 0u64;
    loop {
        set_thread_errno(0);
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let source_err = std::io::Error::last_os_error();
            if source_err.raw_os_error().unwrap_or(0) != 0 {
                return Err(TsinkError::IoWithPath {
                    path: display.to_path_buf(),
                    source: source_err,
                });
            }
            return Ok((names, false, false, retained_bytes));
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if matches!(name, b"." | b"..") {
            continue;
        }
        if u64::try_from(names.len()).unwrap_or(u64::MAX) >= max_entries {
            return Ok((names, true, false, retained_bytes));
        }
        let modeled = modeled_namespace_entry_bytes(source_display, relative, name);
        let Some(required_retained_bytes) = retained_bytes.checked_add(modeled) else {
            return Ok((names, false, true, retained_bytes));
        };
        if required_retained_bytes > max_retained_bytes {
            return Ok((names, false, true, retained_bytes));
        }
        names.try_reserve_exact(1).map_err(|err| {
            TsinkError::Other(format!(
                "failed reserving bounded directory-entry name for {}: {err}",
                display.display()
            ))
        })?;
        names.push(OsString::from_vec(name.to_vec()));
        retained_bytes = required_retained_bytes;
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))
))]
fn read_directory_names_nofollow(
    _directory: &File,
    _display: &Path,
    _relative: &Path,
    _source_display: &Path,
    _max_entries: u64,
    _max_retained_bytes: u64,
) -> Result<(Vec<OsString>, bool, bool, u64)> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative directory enumeration is unsupported on this Unix platform".to_string(),
    ))
}

#[cfg(not(unix))]
fn read_directory_names_nofollow(
    _directory: &File,
    _display: &Path,
    _relative: &Path,
    _source_display: &Path,
    _max_entries: u64,
    _max_retained_bytes: u64,
) -> Result<(Vec<std::ffi::OsString>, bool, bool, u64)> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative directory enumeration is unsupported on this platform".to_string(),
    ))
}

fn modeled_namespace_entry_bytes(
    source_display: &Path,
    relative_parent: &Path,
    name: &[u8],
) -> u64 {
    let parent_len = relative_parent.as_os_str().as_encoded_bytes().len();
    let relative_len = parent_len
        .saturating_add(usize::from(parent_len > 0))
        .saturating_add(name.len());
    let source_len = source_display.as_os_str().as_encoded_bytes().len();
    let absolute_len = source_len
        .saturating_add(usize::from(source_len > 0))
        .saturating_add(relative_len);
    let inline = std::mem::size_of::<DiscoveredEntry>()
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<OsString>().saturating_mul(2));
    u64::try_from(
        inline
            .saturating_add(name.len())
            .saturating_add(relative_len)
            .saturating_add(absolute_len),
    )
    .unwrap_or(u64::MAX)
}

fn fallible_path_join(base: &Path, suffix: &Path, operation: &str) -> Result<PathBuf> {
    let required = base
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .saturating_add(suffix.as_os_str().as_encoded_bytes().len())
        .saturating_add(1);
    let mut joined = PathBuf::new();
    joined.try_reserve(required).map_err(|err| {
        TsinkError::Other(format!(
            "failed reserving bounded path for {operation}: {err}"
        ))
    })?;
    joined.push(base);
    joined.push(suffix);
    Ok(joined)
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn stat_entry_nofollow(parent: &File, name: &OsStr, display: &Path) -> Result<EntryStat> {
    let name = c_name(name, display)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let stat = unsafe { stat.assume_init() };
    let file_type = stat.st_mode & libc::S_IFMT;
    let kind = if file_type == libc::S_IFLNK {
        EntryKind::LinkLike
    } else if file_type == libc::S_IFREG {
        EntryKind::File
    } else if file_type == libc::S_IFDIR {
        EntryKind::Directory
    } else {
        EntryKind::Other
    };
    Ok(EntryStat {
        kind,
        len: u64::try_from(stat.st_size).unwrap_or(0),
        identity: FileIdentity {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
        },
    })
}

#[cfg(not(unix))]
fn stat_entry_nofollow(_parent: &File, _name: &OsStr, _display: &Path) -> Result<EntryStat> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative metadata inspection is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn metadata_identity(metadata: &Metadata) -> FileIdentity {
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn metadata_identity(_metadata: &Metadata) -> FileIdentity {
    FileIdentity {
        device: 0,
        inode: 0,
    }
}

fn verify_opened_identity(file: &File, expected: EntryStat, display: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: source_err,
        })?;
    let kind_matches = match expected.kind {
        EntryKind::File => metadata.file_type().is_file(),
        EntryKind::Directory => metadata.file_type().is_dir(),
        EntryKind::LinkLike | EntryKind::Other => false,
    };
    if !kind_matches
        || metadata.len() != expected.len
        || metadata_identity(&metadata) != expected.identity
    {
        return Err(TsinkError::DataCorruption(format!(
            "filesystem entry identity, type, or length changed while opening: {}",
            display.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn relative_normal_components(path: &Path) -> Result<Vec<&OsStr>> {
    let mut out = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(name) => out.push(name),
            _ => {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "handle-relative salvage path is not a normalized relative path: {}",
                    path.display()
                )))
            }
        }
    }
    if out.is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "handle-relative salvage path is empty".to_string(),
        ));
    }
    Ok(out)
}

#[cfg(unix)]
fn open_directory_component_nofollow(parent: &File, name: &OsStr, display: &Path) -> Result<File> {
    let name = c_name(name, display)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_relative_parent_nofollow(
    root: &File,
    relative: &Path,
    display: &Path,
) -> Result<(File, CString)> {
    let components = relative_normal_components(relative)?;
    let mut parent = root
        .try_clone()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: source_err,
        })?;
    let mut traversed = PathBuf::new();
    for component in &components[..components.len() - 1] {
        traversed.push(component);
        parent = open_directory_component_nofollow(
            &parent,
            component,
            &display.parent().unwrap_or(display).join(&traversed),
        )?;
    }
    Ok((
        parent,
        c_name(
            components
                .last()
                .expect("relative components checked nonempty"),
            display,
        )?,
    ))
}

#[cfg(unix)]
fn open_relative_directory_nofollow(root: &File, relative: &Path, display: &Path) -> Result<File> {
    let (parent, name) = open_relative_parent_nofollow(root, relative, display)?;
    let name = OsStr::from_bytes(name.as_bytes());
    open_directory_component_nofollow(&parent, name, display)
}

#[cfg(not(unix))]
fn open_relative_directory_nofollow(
    _root: &File,
    _relative: &Path,
    _display: &Path,
) -> Result<File> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn create_relative_directory_nofollow(root: &File, relative: &Path, display: &Path) -> Result<()> {
    let (parent, name) = open_relative_parent_nofollow(root, relative, display)?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o755) };
    if result != 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let opened =
        open_directory_component_nofollow(&parent, OsStr::from_bytes(name.as_bytes()), display)?;
    opened
        .sync_all()
        .and_then(|_| parent.sync_all())
        .map_err(|source_err| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: source_err,
        })
}

#[cfg(not(unix))]
fn create_relative_directory_nofollow(
    _root: &File,
    _relative: &Path,
    _display: &Path,
) -> Result<()> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn open_relative_regular_file_nofollow(
    root: &File,
    relative: &Path,
    display: &Path,
) -> Result<File> {
    let (parent, name) = open_relative_parent_nofollow(root, relative, display)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: source_err,
        })?;
    if !metadata.file_type().is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "handle-relative salvage source is not a regular file: {}",
            display.display()
        )));
    }
    Ok(file)
}

#[cfg(unix)]
fn create_relative_regular_file_nofollow(
    root: &File,
    relative: &Path,
    display: &Path,
    mode: libc::mode_t,
) -> Result<(File, File)> {
    let (parent, name) = open_relative_parent_nofollow(root, relative, display)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            libc::c_uint::from(mode),
        )
    };
    if fd < 0 {
        return Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok((unsafe { File::from_raw_fd(fd) }, parent))
}

#[cfg(not(unix))]
fn create_relative_regular_file_nofollow(
    _root: &File,
    _relative: &Path,
    _display: &Path,
    _mode: u32,
) -> Result<(File, File)> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn copy_relative_regular_file_nofollow(
    source_root: &File,
    staging_root: &File,
    source_display: &Path,
    staging_display: &Path,
    entry: &DiscoveredEntry,
    copy_len: u64,
) -> Result<()> {
    if copy_len > entry.len {
        return Err(TsinkError::DataCorruption(
            "salvage copy prefix exceeds source file".to_string(),
        ));
    }
    let source_path = source_display.join(&entry.relative);
    let destination_path = staging_display.join(&entry.relative);
    let mut source =
        open_relative_regular_file_nofollow(source_root, &entry.relative, &source_path)?;
    let before = source
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: source_path.clone(),
            source: source_err,
        })?;
    if before.len() != entry.len {
        return Err(TsinkError::DataCorruption(format!(
            "salvage source length changed before copy: {}",
            source_path.display()
        )));
    }
    let mode = before.mode() & 0o7777;
    let (mut destination, destination_parent) = create_relative_regular_file_nofollow(
        staging_root,
        &entry.relative,
        &destination_path,
        0o600,
    )?;
    let mut remaining = copy_len;
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    while remaining > 0 {
        let take = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        source
            .read_exact(&mut buffer[..take])
            .map_err(|source_err| TsinkError::IoWithPath {
                path: source_path.clone(),
                source: source_err,
            })?;
        destination
            .write_all(&buffer[..take])
            .map_err(|source_err| TsinkError::IoWithPath {
                path: destination_path.clone(),
                source: source_err,
            })?;
        remaining -= take as u64;
    }
    if copy_len == entry.len {
        let mut probe = [0u8; 1];
        if source
            .read(&mut probe)
            .map_err(|source_err| TsinkError::IoWithPath {
                path: source_path.clone(),
                source: source_err,
            })?
            != 0
        {
            return Err(TsinkError::DataCorruption(format!(
                "salvage source grew during copy: {}",
                source_path.display()
            )));
        }
    }
    let after = source
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: source_path.clone(),
            source: source_err,
        })?;
    if !after.file_type().is_file()
        || after.len() != entry.len
        || before.dev() != after.dev()
        || before.ino() != after.ino()
    {
        return Err(TsinkError::DataCorruption(format!(
            "salvage source identity changed during copy: {}",
            source_path.display()
        )));
    }
    destination
        .set_permissions(std::fs::Permissions::from_mode(mode))
        .and_then(|_| destination.flush())
        .and_then(|_| destination.sync_all())
        .and_then(|_| destination_parent.sync_all())
        .map_err(|source_err| TsinkError::IoWithPath {
            path: destination_path,
            source: source_err,
        })
}

#[cfg(not(unix))]
fn copy_relative_regular_file_nofollow(
    _source_root: &File,
    _staging_root: &File,
    _source_display: &Path,
    _staging_display: &Path,
    _entry: &DiscoveredEntry,
    _copy_len: u64,
) -> Result<()> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn handle_relative_entry_exists(parent: &File, name: &OsStr) -> Result<bool> {
    let display = Path::new(name);
    let name = c_name(name, display)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(false)
    } else {
        Err(TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source: error,
        })
    }
}

#[cfg(not(unix))]
fn handle_relative_entry_exists(_parent: &File, _name: &OsStr) -> Result<bool> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn handle_relative_directory_identity_matches(
    parent: &File,
    name: &OsStr,
    expected: &same_file::Handle,
) -> Result<bool> {
    let display = Path::new(name);
    let current = match open_directory_component_nofollow(parent, name, display) {
        Ok(file) => file,
        Err(TsinkError::IoWithPath { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(false);
        }
        Err(err) => return Err(err),
    };
    let current =
        same_file::Handle::from_file(current).map_err(|source| TsinkError::IoWithPath {
            path: display.to_path_buf(),
            source,
        })?;
    Ok(&current == expected)
}

#[cfg(not(unix))]
fn handle_relative_directory_identity_matches(
    _parent: &File,
    _name: &OsStr,
    _expected: &same_file::Handle,
) -> Result<bool> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn create_handle_relative_salvage_staging(
    parent: &File,
    parent_display: &Path,
) -> Result<(PathBuf, OsString, File)> {
    for _ in 0..256 {
        let nonce = SALVAGE_STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = OsString::from(format!(
            ".tmp-tsink-data-salvage-{}-{nonce:016x}",
            std::process::id()
        ));
        let display = parent_display.join(&name);
        let encoded = c_name(&name, &display)?;
        let result = unsafe { libc::mkdirat(parent.as_raw_fd(), encoded.as_ptr(), 0o700) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                continue;
            }
            return Err(TsinkError::IoWithPath {
                path: display,
                source: error,
            });
        }
        let directory =
            open_directory_component_nofollow(parent, &name, &display).map_err(|err| {
                TsinkError::Other(format!(
                    "{err}; staging ownership could not be established, retained path: {}",
                    display.display()
                ))
            })?;
        parent
            .sync_all()
            .map_err(|source_err| TsinkError::IoWithPath {
                path: display.clone(),
                source: source_err,
            })?;
        return Ok((display, name, directory));
    }
    Err(TsinkError::Other(
        "unable to allocate a unique bounded salvage staging directory".to_string(),
    ))
}

#[cfg(not(unix))]
fn create_handle_relative_salvage_staging(
    _parent: &File,
    _parent_display: &Path,
) -> Result<(PathBuf, std::ffi::OsString, File)> {
    Err(TsinkError::InvalidConfiguration(
        "handle-relative salvage is unsupported on this platform".to_string(),
    ))
}

struct SalvagePublicationError {
    error: TsinkError,
    published: bool,
}

#[cfg(unix)]
fn rename_handle_relative_noreplace(
    parent: &File,
    source_name: &OsStr,
    destination_name: &OsStr,
) -> std::result::Result<(), SalvagePublicationError> {
    let source =
        c_name(source_name, Path::new(source_name)).map_err(|error| SalvagePublicationError {
            error,
            published: false,
        })?;
    let destination = c_name(destination_name, Path::new(destination_name)).map_err(|error| {
        SalvagePublicationError {
            error,
            published: false,
        }
    })?;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    let result = unsafe {
        libc::renameatx_np(
            parent.as_raw_fd(),
            source.as_ptr(),
            parent.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            source.as_ptr(),
            parent.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        ) as libc::c_int
    };
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
    let result = {
        return Err(SalvagePublicationError {
            error: TsinkError::InvalidConfiguration(
                "atomic handle-relative no-replace rename is unsupported on this Unix platform"
                    .to_string(),
            ),
            published: false,
        });
    };
    if result != 0 {
        let source_err = std::io::Error::last_os_error();
        if source_err.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(SalvagePublicationError {
                error: TsinkError::InvalidConfiguration(format!(
                    "salvage destination already exists and will not be replaced: {}",
                    Path::new(destination_name).display()
                )),
                published: false,
            });
        }
        return Err(SalvagePublicationError {
            error: TsinkError::IoWithPath {
                path: PathBuf::from(destination_name),
                source: source_err,
            },
            published: false,
        });
    }
    parent
        .sync_all()
        .map_err(|source_err| SalvagePublicationError {
            error: TsinkError::IoWithPath {
                path: PathBuf::from(destination_name),
                source: source_err,
            },
            published: true,
        })
}

#[cfg(not(unix))]
fn rename_handle_relative_noreplace(
    _parent: &File,
    _source_name: &OsStr,
    _destination_name: &OsStr,
) -> std::result::Result<(), SalvagePublicationError> {
    Err(SalvagePublicationError {
        error: TsinkError::InvalidConfiguration(
            "handle-relative salvage is unsupported on this platform".to_string(),
        ),
        published: false,
    })
}

fn encode_salvaged_wal_marker(highwater: SalvagedWalHighwater) -> Vec<u8> {
    let highwater = WalHighWatermark {
        segment: highwater.segment,
        frame: highwater.frame,
    };
    encode_published_highwater_record(PublishedHighwaterRecord {
        highwater,
        reset_through: Some(highwater),
    })
}

fn wal_segment_id_from_relative(relative: &Path) -> Option<u64> {
    if relative.parent() != Some(Path::new("wal")) {
        return None;
    }
    let name = relative.file_name()?.to_str()?;
    if name == "wal.log" {
        Some(0)
    } else {
        parse_exact_hex_file(name, "wal-", ".log")
    }
}

fn render_required_salvage_path(
    path: &Path,
    limits: DataDirectoryInspectionLimits,
) -> Result<String> {
    let raw = path.as_os_str().as_encoded_bytes();
    let encoded_len = rendered_path_len(raw);
    if encoded_len > limits.max_path_bytes as usize
        || u64::try_from(encoded_len).unwrap_or(u64::MAX) > limits.max_retained_path_bytes
    {
        return Err(TsinkError::InvalidConfiguration(format!(
            "salvage destination path exceeds inspection report path limits: {}",
            path.display()
        )));
    }
    let mut rendered = String::new();
    rendered.try_reserve(encoded_len).map_err(|err| {
        TsinkError::Other(format!(
            "failed reserving bounded salvage path encoding: {err}"
        ))
    })?;
    for byte in raw.iter().copied() {
        push_rendered_path_byte(&mut rendered, byte);
    }
    Ok(rendered)
}

fn rendered_path_len(raw: &[u8]) -> usize {
    raw.iter().copied().fold(0usize, |total, byte| {
        total.saturating_add(rendered_path_byte_len(byte))
    })
}

fn rendered_path_byte_len(byte: u8) -> usize {
    if is_direct_rendered_path_byte(byte) {
        1
    } else {
        3
    }
}

fn is_direct_rendered_path_byte(byte: u8) -> bool {
    (byte.is_ascii_graphic() || byte == b' ') && !matches!(byte, b'%' | b'"' | b'\\')
}

fn push_rendered_path_byte(rendered: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    if is_direct_rendered_path_byte(byte) {
        rendered.push(char::from(byte));
    } else {
        rendered.push('%');
        rendered.push(char::from(HEX[usize::from(byte >> 4)]));
        rendered.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_report(
    ctx: InspectionContext,
    source_path: String,
    format: DataDirectoryFormat,
    manifest: Option<InspectedDataDirectoryManifest>,
    registry: InspectedSeriesRegistry,
    wal_published: InspectedWalPublishedMarker,
    wal_segments: Vec<InspectedWalSegment>,
    segments: Vec<InspectedPersistedSegment>,
) -> DataDirectoryInspectionReport {
    let complete = ctx.bounds_hit.is_empty();
    let health = if !complete {
        InspectionHealth::Incomplete
    } else if ctx.findings.is_empty() && ctx.discarded_ranges.is_empty() {
        InspectionHealth::Clean
    } else {
        InspectionHealth::FindingsPresent
    };
    DataDirectoryInspectionReport {
        report_schema_version: DATA_DIRECTORY_INSPECTION_REPORT_SCHEMA_VERSION,
        source_path,
        format,
        health,
        manifest,
        registry,
        wal_published,
        wal_segments,
        segments,
        findings: ctx.findings,
        discarded_ranges: ctx.discarded_ranges,
        work: ctx.work,
        completeness: InspectionCompleteness {
            complete,
            bounds_hit: ctx.bounds_hit.into_iter().map(str::to_string).collect(),
        },
    }
}

fn walk_namespace(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    directory: &File,
    relative: &Path,
    depth: u16,
    out: &mut Vec<DiscoveredEntry>,
) -> Result<bool> {
    if depth > ctx.limits.max_namespace_depth {
        ctx.hit("max_namespace_depth");
        return Ok(false);
    }
    ctx.work.directories_visited = ctx.work.directories_visited.saturating_add(1);
    let directory_display = source.display.join(relative);
    let (mut local, entries_exceeded, bytes_exceeded, retained_bytes) =
        read_directory_names_nofollow(
            directory,
            &directory_display,
            relative,
            source.display,
            ctx.limits
                .max_namespace_entries
                .saturating_sub(ctx.work.namespace_entries),
            ctx.limits
                .max_namespace_retained_bytes
                .saturating_sub(ctx.work.namespace_retained_bytes),
        )?;
    ctx.work.namespace_retained_bytes = ctx
        .work
        .namespace_retained_bytes
        .checked_add(retained_bytes)
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    ctx.work.namespace_entries = ctx
        .work
        .namespace_entries
        .checked_add(u64::try_from(local.len()).unwrap_or(u64::MAX))
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    if entries_exceeded {
        ctx.hit("max_namespace_entries");
        return Ok(false);
    }
    if bytes_exceeded {
        ctx.hit("max_namespace_retained_bytes");
        return Ok(false);
    }
    local.sort_by(|left, right| left.as_encoded_bytes().cmp(right.as_encoded_bytes()));
    for name in local {
        let entry_relative =
            fallible_path_join(relative, Path::new(&name), "namespace relative path")?;
        let path = fallible_path_join(source.display, &entry_relative, "namespace display path")?;
        let stat = stat_entry_nofollow(directory, &name, &path)?;
        let kind = stat.kind;
        out.try_reserve_exact(1).map_err(|err| {
            TsinkError::Other(format!(
                "failed reserving bounded namespace entry for {}: {err}",
                path.display()
            ))
        })?;
        out.push(DiscoveredEntry {
            path: path.clone(),
            relative: entry_relative.clone(),
            kind,
            len: stat.len,
            identity: stat.identity,
        });
        match kind {
            EntryKind::LinkLike => ctx.finding(
                "namespace.link_like_entry",
                InspectionSeverity::Error,
                Some(&entry_relative),
                "link-like or reparse-point entry was not followed",
                None,
            ),
            EntryKind::Other => ctx.finding(
                "namespace.unsupported_entry_type",
                InspectionSeverity::Error,
                Some(&entry_relative),
                "entry is neither a regular file nor a real directory",
                None,
            ),
            EntryKind::Directory => {
                let child = open_directory_component_nofollow(directory, &name, &path)?;
                verify_opened_identity(&child, stat, &path)?;
                if !walk_namespace(
                    ctx,
                    source,
                    &child,
                    &entry_relative,
                    depth.saturating_add(1),
                    out,
                )? {
                    return Ok(false);
                }
            }
            EntryKind::File => {}
        }
    }
    Ok(true)
}

fn inspect_data_manifest(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
) -> Result<(DataDirectoryFormat, Option<InspectedDataDirectoryManifest>)> {
    let Some(entry) = find_entry(entries, Path::new(DATA_DIRECTORY_MANIFEST_FILE_NAME)) else {
        return Ok((
            if entries.iter().all(is_ignorable_empty_entry) {
                DataDirectoryFormat::Empty
            } else {
                DataDirectoryFormat::UnknownForeign
            },
            None,
        ));
    };
    if entry.kind != EntryKind::File {
        ctx.finding(
            "format.manifest_not_regular",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "data-directory manifest is not a regular non-link file",
            None,
        );
        return Ok((DataDirectoryFormat::Corrupt, None));
    }
    if entry.len > DATA_DIRECTORY_MANIFEST_MAX_BYTES {
        ctx.finding(
            "format.manifest_oversized",
            InspectionSeverity::Error,
            Some(&entry.relative),
            format!(
                "manifest length {} exceeds format limit {DATA_DIRECTORY_MANIFEST_MAX_BYTES}",
                entry.len
            ),
            None,
        );
        return Ok((DataDirectoryFormat::Corrupt, None));
    }
    let Some(bytes) = read_regular_file(ctx, source, entry, entry.len)? else {
        return Ok((DataDirectoryFormat::Corrupt, None));
    };
    let probe = match serde_json::from_slice::<ManifestHeaderProbe>(&bytes) {
        Ok(probe) => probe,
        Err(err) => {
            ctx.finding(
                "format.manifest_json_corrupt",
                InspectionSeverity::Error,
                Some(&entry.relative),
                format!("manifest header cannot be decoded: {err}"),
                None,
            );
            return Ok((DataDirectoryFormat::Corrupt, None));
        }
    };
    let probe_report = || InspectedDataDirectoryManifest {
        magic: Some(bounded_message(probe.magic.clone())),
        manifest_schema_version: Some(probe.manifest_schema_version),
        storage_format_version: Some(probe.payload.storage_format_version),
        minimum_reader_storage_format_version: None,
        creating_tsink_version: None,
        last_successfully_opened_tsink_version: None,
        format_affecting_features: Vec::new(),
        timestamp_precision: None,
        chunk_point_capacity: None,
        partition_window_timestamp_units: None,
        checksum_valid: None,
    };
    if probe.magic != DATA_DIRECTORY_MAGIC {
        ctx.finding(
            "format.manifest_magic_invalid",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "data-directory manifest magic is invalid",
            None,
        );
        return Ok((DataDirectoryFormat::Corrupt, Some(probe_report())));
    }
    if probe.manifest_schema_version > DATA_DIRECTORY_MANIFEST_SCHEMA_VERSION
        || probe.payload.storage_format_version > crate::engine::STORAGE_FORMAT_VERSION
    {
        ctx.finding(
            "format.unsupported_newer",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "data-directory manifest requires a newer reader",
            None,
        );
        return Ok((DataDirectoryFormat::UnsupportedNewer, Some(probe_report())));
    }
    let envelope = match serde_json::from_slice::<ManifestEnvelope>(&bytes) {
        Ok(envelope) => envelope,
        Err(err) => {
            ctx.finding(
                "format.manifest_json_corrupt",
                InspectionSeverity::Error,
                Some(&entry.relative),
                format!("manifest JSON cannot be decoded: {err}"),
                None,
            );
            return Ok((DataDirectoryFormat::Corrupt, None));
        }
    };
    let encoded_payload = serde_json::to_vec(&envelope.payload).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "inspection could not canonicalize data-directory manifest payload: {err}"
        ))
    })?;
    if !ctx.admit_hash(u64::try_from(encoded_payload.len()).unwrap_or(u64::MAX)) {
        return Ok((DataDirectoryFormat::Corrupt, Some(probe_report())));
    }
    let checksum_valid = crc32fast::hash(&encoded_payload) == envelope.payload_crc32;
    let inspected = InspectedDataDirectoryManifest {
        magic: Some(bounded_message(envelope.magic.clone())),
        manifest_schema_version: Some(envelope.manifest_schema_version),
        storage_format_version: Some(envelope.payload.storage_format_version),
        minimum_reader_storage_format_version: Some(
            envelope.payload.minimum_reader_storage_format_version,
        ),
        creating_tsink_version: envelope
            .payload
            .creating_tsink_version
            .as_deref()
            .map(|value| bounded_message(value.to_string())),
        last_successfully_opened_tsink_version: envelope
            .payload
            .last_successfully_opened_tsink_version
            .as_deref()
            .map(|value| bounded_message(value.to_string())),
        format_affecting_features: envelope
            .payload
            .format_affecting_features
            .iter()
            .take(64)
            .map(|value| bounded_message(value.clone()))
            .collect(),
        timestamp_precision: Some(envelope.payload.timestamp_precision.as_str().to_string()),
        chunk_point_capacity: Some(envelope.payload.chunk_point_capacity),
        partition_window_timestamp_units: Some(envelope.payload.partition_window_timestamp_units),
        checksum_valid: Some(checksum_valid),
    };
    if !checksum_valid {
        ctx.finding(
            "format.manifest_checksum_mismatch",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "data-directory manifest payload CRC32 does not match",
            None,
        );
        return Ok((DataDirectoryFormat::Corrupt, Some(inspected)));
    }
    if envelope.payload.minimum_reader_storage_format_version
        > crate::engine::STORAGE_FORMAT_VERSION
    {
        ctx.finding(
            "format.unsupported_newer",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "data-directory manifest requires a newer reader",
            None,
        );
        return Ok((DataDirectoryFormat::UnsupportedNewer, Some(inspected)));
    }
    if envelope.manifest_schema_version != DATA_DIRECTORY_MANIFEST_SCHEMA_VERSION
        || envelope.payload.storage_format_version != crate::engine::STORAGE_FORMAT_VERSION
        || envelope.payload.minimum_reader_storage_format_version
            > envelope.payload.storage_format_version
    {
        ctx.finding(
            "format.manifest_version_invalid",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "manifest schema or storage-format relationship is unsupported",
            None,
        );
        return Ok((DataDirectoryFormat::Corrupt, Some(inspected)));
    }
    let exact_features = envelope
        .payload
        .format_affecting_features
        .iter()
        .map(String::as_str)
        .eq(CURRENT_FORMAT_FEATURES);
    if !exact_features
        || envelope.payload.chunk_point_capacity == 0
        || envelope.payload.partition_window_timestamp_units <= 0
    {
        ctx.finding(
            "format.manifest_configuration_invalid",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "manifest features or immutable numeric configuration are invalid",
            None,
        );
        return Ok((DataDirectoryFormat::Corrupt, Some(inspected)));
    }
    Ok((DataDirectoryFormat::CurrentManifest, Some(inspected)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedReportPreflightError {
    ReportItems,
    RetainedStrings,
    StringLength,
    SyntaxEnvelope,
}

impl PersistedReportPreflightError {
    fn message(self) -> &'static str {
        match self {
            Self::ReportItems => {
                "persisted salvage report arrays exceed max_report_items before decoding"
            }
            Self::RetainedStrings => {
                "persisted salvage report strings exceed the bounded decode-memory allowance"
            }
            Self::StringLength => {
                "persisted salvage report contains a string longer than the bounded per-string allowance"
            }
            Self::SyntaxEnvelope => {
                "persisted salvage report uses unsupported escaping or excessive JSON nesting"
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum JsonContainer {
    Object,
    Array { expects_value: bool },
}

#[derive(Debug, Clone, Copy)]
struct JsonPreflightStats {
    array_items: u64,
    string_bytes: u64,
}

fn preflight_persisted_salvage_report_json(
    bytes: &[u8],
    limits: DataDirectoryInspectionLimits,
) -> std::result::Result<(), PersistedReportPreflightError> {
    const FIXED_STRING_ALLOWANCE: u64 = 1024;
    const METADATA_STRING_ALLOWANCE_PER_ITEM: u64 = 256;

    let metadata_allowance = limits
        .max_report_items
        .checked_mul(METADATA_STRING_ALLOWANCE_PER_ITEM)
        .and_then(|bytes| bytes.checked_add(FIXED_STRING_ALLOWANCE))
        .ok_or(PersistedReportPreflightError::RetainedStrings)?;
    let total_string_limit = limits
        .max_retained_path_bytes
        .checked_add(metadata_allowance)
        .ok_or(PersistedReportPreflightError::RetainedStrings)?;
    let individual_string_limit = u64::from(limits.max_path_bytes).max(256);
    preflight_json_collections(
        bytes,
        limits.max_report_items,
        individual_string_limit,
        total_string_limit,
    )
    .map(|_| ())
}

fn preflight_json_collections(
    bytes: &[u8],
    max_array_items: u64,
    individual_string_limit: u64,
    total_string_limit: u64,
) -> std::result::Result<JsonPreflightStats, PersistedReportPreflightError> {
    const MAX_JSON_DEPTH: usize = 32;
    let mut containers = [JsonContainer::Object; MAX_JSON_DEPTH];
    let mut depth = 0usize;
    let mut in_string = false;
    let mut string_len = 0u64;
    let mut total_string_bytes = 0u64;
    let mut array_items = 0u64;

    for byte in bytes.iter().copied() {
        if in_string {
            match byte {
                b'\\' => return Err(PersistedReportPreflightError::SyntaxEnvelope),
                b'"' => {
                    in_string = false;
                    total_string_bytes = total_string_bytes
                        .checked_add(string_len)
                        .ok_or(PersistedReportPreflightError::RetainedStrings)?;
                    if total_string_bytes > total_string_limit {
                        return Err(PersistedReportPreflightError::RetainedStrings);
                    }
                    string_len = 0;
                }
                _ => {
                    string_len = string_len
                        .checked_add(1)
                        .ok_or(PersistedReportPreflightError::StringLength)?;
                    if string_len > individual_string_limit {
                        return Err(PersistedReportPreflightError::StringLength);
                    }
                }
            }
            continue;
        }

        if depth > 0 {
            if let JsonContainer::Array { expects_value } = &mut containers[depth - 1] {
                if *expects_value && !byte.is_ascii_whitespace() && byte != b']' {
                    array_items = array_items
                        .checked_add(1)
                        .ok_or(PersistedReportPreflightError::ReportItems)?;
                    if array_items > max_array_items {
                        return Err(PersistedReportPreflightError::ReportItems);
                    }
                    *expects_value = false;
                }
            }
        }

        match byte {
            b'"' => {
                in_string = true;
                string_len = 0;
            }
            b'[' => {
                if depth == MAX_JSON_DEPTH {
                    return Err(PersistedReportPreflightError::SyntaxEnvelope);
                }
                containers[depth] = JsonContainer::Array {
                    expects_value: true,
                };
                depth += 1;
            }
            b'{' => {
                if depth == MAX_JSON_DEPTH {
                    return Err(PersistedReportPreflightError::SyntaxEnvelope);
                }
                containers[depth] = JsonContainer::Object;
                depth += 1;
            }
            b',' if depth > 0 => {
                if let JsonContainer::Array { expects_value } = &mut containers[depth - 1] {
                    *expects_value = true;
                }
            }
            b']' => {
                if depth == 0 || !matches!(containers[depth - 1], JsonContainer::Array { .. }) {
                    return Err(PersistedReportPreflightError::SyntaxEnvelope);
                }
                depth -= 1;
            }
            b'}' => {
                if depth == 0 || !matches!(containers[depth - 1], JsonContainer::Object) {
                    return Err(PersistedReportPreflightError::SyntaxEnvelope);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if in_string || depth != 0 {
        return Err(PersistedReportPreflightError::SyntaxEnvelope);
    }
    Ok(JsonPreflightStats {
        array_items,
        string_bytes: total_string_bytes,
    })
}

fn canonical_persisted_wal_segment_id(path: &str) -> Option<u64> {
    if path == "wal/wal.log" {
        return Some(0);
    }
    let encoded = path.strip_prefix("wal/wal-")?.strip_suffix(".log")?;
    if encoded.len() != 16
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn persisted_salvage_evidence_is_valid(report: &mut PersistedSalvageReport) -> bool {
    report.discarded_ranges.sort_by(|left, right| {
        (
            &left.path,
            &left.reason_code,
            left.bytes.start,
            left.bytes.end,
            left.first_frame,
            left.last_frame,
        )
            .cmp(&(
                &right.path,
                &right.reason_code,
                right.bytes.start,
                right.bytes.end,
                right.first_frame,
                right.last_frame,
            ))
    });
    report.omitted_wal_paths.sort();

    let mut omitted_index = 0usize;
    let mut retained_reset_count = 0usize;
    let mut full_reset_count = 0usize;
    let mut has_source_corruption = false;
    let mut range_index = 0usize;
    while range_index < report.discarded_ranges.len() {
        let path = report.discarded_ranges[range_index].path.as_str();
        let Some(segment_id) = canonical_persisted_wal_segment_id(path) else {
            return false;
        };
        let group_end = report.discarded_ranges[range_index..]
            .iter()
            .position(|range| range.path != path)
            .map_or(report.discarded_ranges.len(), |offset| range_index + offset);
        let group = &report.discarded_ranges[range_index..group_end];
        let mut group_full_reset_end = None;
        let mut previous_reason: Option<&str> = None;
        for range in group {
            let is_full_reset = range.reason_code == "wal.salvage_v1_full_reset";
            if range.bytes.start > range.bytes.end
                || (range.bytes.start == range.bytes.end && !is_full_reset)
                || matches!(
                    (range.first_frame, range.last_frame),
                    (Some(first), Some(last)) if first > last
                )
                || previous_reason == Some(range.reason_code.as_str())
            {
                return false;
            }
            previous_reason = Some(range.reason_code.as_str());
            match range.reason_code.as_str() {
                "wal.salvage_v1_full_reset" => {
                    if range.bytes.start != 0 || group_full_reset_end.is_some() {
                        return false;
                    }
                    group_full_reset_end = Some(range.bytes.end);
                    full_reset_count = full_reset_count.saturating_add(1);
                }
                "wal.corrupt_tail" | "wal.mid_log_corruption" => {
                    has_source_corruption = true;
                }
                "wal.after_first_unsafe_range" | "wal.unpublished_suffix" => {}
                _ => return false,
            }
        }
        let Some(full_reset_end) = group_full_reset_end else {
            return false;
        };
        if group.iter().any(|range| range.bytes.end > full_reset_end) {
            return false;
        }
        if segment_id < report.retained_wal_highwater.segment {
            return false;
        }
        if segment_id == report.retained_wal_highwater.segment {
            retained_reset_count = retained_reset_count.saturating_add(1);
        } else {
            let Some(omitted) = report.omitted_wal_paths.get(omitted_index) else {
                return false;
            };
            if omitted != path {
                return false;
            }
            omitted_index = omitted_index.saturating_add(1);
        }
        range_index = group_end;
    }

    if full_reset_count == 0
        || retained_reset_count != 1
        || !has_source_corruption
        || omitted_index != report.omitted_wal_paths.len()
    {
        return false;
    }

    let mut published_marker_dispositions = 0usize;
    let mut lock_dispositions = 0usize;
    for disposition in &report.path_dispositions {
        match (
            disposition.path.as_str(),
            disposition.disposition,
            disposition.reason_code.as_str(),
        ) {
            (".tsink.lock", SalvagePathDispositionKind::Omitted, "operational.lock_not_copied") => {
                lock_dispositions = lock_dispositions.saturating_add(1)
            }
            (
                "wal/wal.published",
                SalvagePathDispositionKind::Replaced,
                "wal.published_rewritten_to_retained_highwater",
            ) => {
                published_marker_dispositions = published_marker_dispositions.saturating_add(1);
            }
            _ => return false,
        }
    }
    published_marker_dispositions == 1
        && lock_dispositions <= 1
        && report.path_dispositions.len()
            == published_marker_dispositions.saturating_add(lock_dispositions)
}

fn inspect_persisted_salvage_report(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
    published: &InspectedWalPublishedMarker,
    wal_segments: &[InspectedWalSegment],
) -> Result<()> {
    let Some(entry) = find_entry(entries, Path::new(SALVAGE_REPORT_FILE_NAME)) else {
        return Ok(());
    };
    if entry.kind != EntryKind::File {
        ctx.finding(
            "salvage.report_not_regular",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "persisted salvage report is not a regular non-link file",
            None,
        );
        return Ok(());
    }
    if entry.len > ctx.limits.max_catalog_bytes {
        ctx.finding(
            "salvage.report_exceeds_limit",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "persisted salvage report exceeds max_catalog_bytes",
            None,
        );
        ctx.hit("max_catalog_bytes");
        return Ok(());
    }
    let Some(bytes) = read_regular_file(ctx, source, entry, entry.len)? else {
        return Ok(());
    };
    if let Err(preflight) = preflight_persisted_salvage_report_json(&bytes, ctx.limits) {
        match preflight {
            PersistedReportPreflightError::ReportItems => ctx.hit("max_report_items"),
            PersistedReportPreflightError::RetainedStrings => ctx.hit("max_retained_path_bytes"),
            PersistedReportPreflightError::StringLength => ctx.hit("max_path_bytes"),
            PersistedReportPreflightError::SyntaxEnvelope => {}
        }
        ctx.finding(
            "salvage.report_decode_limit_exceeded",
            InspectionSeverity::Warning,
            Some(&entry.relative),
            preflight.message(),
            None,
        );
        return Ok(());
    }
    match serde_json::from_slice::<PersistedSalvageReport>(&bytes) {
        Ok(mut report) => {
            let retained_items = report
                .discarded_ranges
                .len()
                .checked_add(report.omitted_wal_paths.len())
                .and_then(|count| count.checked_add(report.path_dispositions.len()))
                .unwrap_or(usize::MAX);
            if u64::try_from(retained_items).unwrap_or(u64::MAX) > ctx.limits.max_report_items {
                ctx.hit("max_report_items");
            }
            let retained_path_bytes = report
                .discarded_ranges
                .iter()
                .map(|range| range.path.len())
                .chain(report.omitted_wal_paths.iter().map(String::len))
                .chain(
                    report
                        .path_dispositions
                        .iter()
                        .map(|disposition| disposition.path.len()),
                )
                .fold(
                    report
                        .source_path
                        .len()
                        .saturating_add(report.destination_path.len()),
                    usize::saturating_add,
                );
            if u64::try_from(retained_path_bytes).unwrap_or(u64::MAX)
                > ctx.limits.max_retained_path_bytes
            {
                ctx.hit("max_retained_path_bytes");
            }
            let evidence_valid = persisted_salvage_evidence_is_valid(&mut report);
            let current_wal_matches = wal_segments.len() == 1
                && wal_segments[0].segment_id == report.retained_wal_highwater.segment
                && wal_segments[0].file_len == 0
                && wal_segments[0].valid_frames == 0
                && published.checksum_valid == Some(true)
                && published.segment == Some(report.retained_wal_highwater.segment)
                && published.frame == Some(0)
                && published.reset_through_segment == Some(report.retained_wal_highwater.segment)
                && published.reset_through_frame == Some(0);
            let invariant_valid = report.report_schema_version
                == DATA_DIRECTORY_INSPECTION_REPORT_SCHEMA_VERSION
                && !report.source_path.is_empty()
                && !report.destination_path.is_empty()
                && report.source_path.len() <= ctx.limits.max_path_bytes as usize
                && report.destination_path.len() <= ctx.limits.max_path_bytes as usize
                && report.retained_wal_highwater.frame == 0
                && evidence_valid
                && current_wal_matches;
            if !invariant_valid {
                ctx.finding(
                    "salvage.report_invariant_invalid",
                    InspectionSeverity::Warning,
                    Some(&entry.relative),
                    "persisted salvage report does not match the current v1 empty-WAL destination invariants",
                    None,
                );
            }
        }
        Err(err) => ctx.finding(
            "salvage.report_corrupt",
            InspectionSeverity::Warning,
            Some(&entry.relative),
            format!("persisted salvage report JSON cannot be decoded: {err}"),
            None,
        ),
    }
    Ok(())
}

fn inspect_registry_snapshot(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
    required: bool,
) -> Result<InspectedSeriesRegistry> {
    let Some(entry) = find_entry(entries, Path::new("series_index.bin")) else {
        if required {
            ctx.finding(
                "registry.snapshot_missing",
                InspectionSeverity::Error,
                Some(Path::new("series_index.bin")),
                "current data directory is missing its series-registry snapshot",
                None,
            );
        }
        return Ok(InspectedSeriesRegistry {
            present: false,
            valid: false,
            series_count: None,
        });
    };
    if entry.kind != EntryKind::File {
        ctx.finding(
            "registry.snapshot_not_regular",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "series-registry snapshot is not a regular non-link file",
            None,
        );
        return Ok(InspectedSeriesRegistry {
            present: true,
            valid: false,
            series_count: None,
        });
    }
    if entry.len > ctx.limits.max_registry_bytes {
        ctx.finding(
            "registry.snapshot_exceeds_limit",
            InspectionSeverity::Error,
            Some(&entry.relative),
            format!(
                "series-registry snapshot length {} exceeds max_registry_bytes {}",
                entry.len, ctx.limits.max_registry_bytes
            ),
            None,
        );
        ctx.hit("max_registry_bytes");
        return Ok(InspectedSeriesRegistry {
            present: true,
            valid: false,
            series_count: None,
        });
    }
    let Some(bytes) = read_regular_file(ctx, source, entry, entry.len)? else {
        return Ok(InspectedSeriesRegistry {
            present: true,
            valid: false,
            series_count: None,
        });
    };
    let modeled = match crate::engine::series::modeled_registry_payload_for_inspection(&bytes) {
        Ok(modeled) => modeled,
        Err(err) => {
            ctx.finding(
                "registry.snapshot_corrupt",
                InspectionSeverity::Error,
                Some(&entry.relative),
                format!("series-registry snapshot cannot be modeled safely: {err}"),
                None,
            );
            return Ok(InspectedSeriesRegistry {
                present: true,
                valid: false,
                series_count: None,
            });
        }
    };
    let modeled_u64 = u64::try_from(modeled).unwrap_or(u64::MAX);
    if modeled_u64 > ctx.limits.max_registry_bytes {
        ctx.hit("max_registry_bytes");
        return Ok(InspectedSeriesRegistry {
            present: true,
            valid: false,
            series_count: None,
        });
    }
    ctx.work.registry_modeled_bytes = modeled_u64;
    if !ctx.admit_hash(entry.len) {
        return Ok(InspectedSeriesRegistry {
            present: true,
            valid: false,
            series_count: None,
        });
    }
    match crate::engine::series::validate_registry_payload_for_inspection(&bytes, modeled) {
        Ok(series_count) => Ok(InspectedSeriesRegistry {
            present: true,
            valid: true,
            series_count: Some(u64::try_from(series_count).unwrap_or(u64::MAX)),
        }),
        Err(err) => {
            ctx.finding(
                "registry.snapshot_corrupt",
                InspectionSeverity::Error,
                Some(&entry.relative),
                format!("series-registry snapshot cannot be strictly decoded: {err}"),
                None,
            );
            Ok(InspectedSeriesRegistry {
                present: true,
                valid: false,
                series_count: None,
            })
        }
    }
}

fn inspect_wal_published(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
) -> Result<InspectedWalPublishedMarker> {
    let relative = Path::new("wal").join("wal.published");
    let Some(entry) = find_entry(entries, &relative) else {
        return Ok(InspectedWalPublishedMarker {
            present: false,
            checksum_valid: None,
            segment: None,
            frame: None,
            reset_through_segment: None,
            reset_through_frame: None,
            frame_type: None,
        });
    };
    if entry.kind != EntryKind::File
        || !matches!(
            entry.len,
            WAL_PUBLISHED_LEGACY_BYTES | WAL_PUBLISHED_V2_BYTES
        )
    {
        ctx.finding(
            "wal.published_invalid_size_or_type",
            InspectionSeverity::Error,
            Some(&entry.relative),
            format!(
                "published marker must be a {WAL_PUBLISHED_LEGACY_BYTES}- or \
                 {WAL_PUBLISHED_V2_BYTES}-byte regular non-link file"
            ),
            None,
        );
        return Ok(InspectedWalPublishedMarker {
            present: true,
            checksum_valid: Some(false),
            segment: None,
            frame: None,
            reset_through_segment: None,
            reset_through_frame: None,
            frame_type: None,
        });
    }
    let Some(bytes) = read_regular_file(ctx, source, entry, WAL_PUBLISHED_MAX_BYTES)? else {
        return Ok(InspectedWalPublishedMarker {
            present: true,
            checksum_valid: None,
            segment: None,
            frame: None,
            reset_through_segment: None,
            reset_through_frame: None,
            frame_type: None,
        });
    };
    let hashed_bytes = entry.len.saturating_sub(4);
    if !ctx.admit_hash(hashed_bytes) {
        return Ok(InspectedWalPublishedMarker {
            present: true,
            checksum_valid: None,
            segment: None,
            frame: None,
            reset_through_segment: None,
            reset_through_frame: None,
            frame_type: None,
        });
    }
    match decode_published_highwater_record(&bytes) {
        Ok(record) => Ok(InspectedWalPublishedMarker {
            present: true,
            checksum_valid: Some(true),
            segment: Some(record.highwater.segment),
            frame: Some(record.highwater.frame),
            reset_through_segment: record.reset_through.map(|highwater| highwater.segment),
            reset_through_frame: record.reset_through.map(|highwater| highwater.frame),
            frame_type: None,
        }),
        Err(_) => {
            ctx.finding(
                "wal.published_corrupt",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "published marker magic or checksum is invalid",
                None,
            );
            Ok(InspectedWalPublishedMarker {
                present: true,
                checksum_valid: Some(false),
                segment: None,
                frame: None,
                reset_through_segment: None,
                reset_through_frame: None,
                frame_type: None,
            })
        }
    }
}

fn inspect_wal_segments(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
    published: &mut InspectedWalPublishedMarker,
) -> Result<Vec<InspectedWalSegment>> {
    let mut canonical = Vec::new();
    let mut ids = BTreeSet::new();
    for entry in entries {
        if entry.kind != EntryKind::File || entry.relative.parent() != Some(Path::new("wal")) {
            continue;
        }
        let Some(name) = entry.relative.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let id = if name == "wal.log" {
            Some(0)
        } else {
            parse_exact_hex_file(name, "wal-", ".log")
        };
        let Some(id) = id else {
            continue;
        };
        if !ids.insert(id) {
            ctx.finding(
                "wal.duplicate_segment_id",
                InspectionSeverity::Error,
                Some(&entry.relative),
                format!("more than one canonical name resolves to WAL segment {id}"),
                None,
            );
            continue;
        }
        canonical.push((id, entry));
    }
    canonical.sort_by_key(|(id, _)| *id);
    let mut out = Vec::new();
    let mut previous_sequence = None;
    for (segment_id, entry) in canonical {
        if ctx.work.wal_segments >= ctx.limits.max_wal_segments {
            ctx.hit("max_wal_segments");
            break;
        }
        if !ctx.reserve_report_item() {
            break;
        }
        let Some(path) = ctx.rendered_path(&entry.relative) else {
            ctx.cancel_report_item();
            break;
        };
        ctx.work.wal_segments += 1;
        let (
            status,
            valid_frames,
            first_valid_frame,
            last_valid_frame,
            first_valid_frame_type,
            last_valid_frame_type,
        ) = inspect_one_wal_segment(
            ctx,
            source,
            entry,
            segment_id,
            published,
            &mut previous_sequence,
        )?;
        out.push(InspectedWalSegment {
            segment_id,
            path,
            file_len: entry.len,
            valid_frames,
            first_valid_frame,
            last_valid_frame,
            first_valid_frame_type,
            last_valid_frame_type,
            status,
        });
    }
    for index in 0..out.len() {
        if out[index].status != WalSegmentStatus::CorruptTail
            || !out[index + 1..].iter().any(|later| later.file_len > 0)
        {
            continue;
        }
        out[index].status = WalSegmentStatus::MidLogCorruption;
        for finding in &mut ctx.findings {
            if finding.path.as_deref() == Some(out[index].path.as_str())
                && matches!(
                    finding.code.as_str(),
                    "wal.corrupt_tail_header" | "wal.corrupt_tail_payload"
                )
            {
                finding.code = "wal.mid_log_truncation_before_later_segment".to_string();
                finding.message = bounded_message(
                    "truncated WAL bytes precede a later nonempty canonical segment".to_string(),
                );
            }
        }
        for range in &mut ctx.discarded_ranges {
            if range.path == out[index].path && range.reason_code == "wal.corrupt_tail" {
                range.reason_code = "wal.mid_log_corruption".to_string();
            }
        }
    }
    if let Some(first_unsafe) = out.iter().position(|segment| {
        matches!(
            segment.status,
            WalSegmentStatus::CorruptTail | WalSegmentStatus::MidLogCorruption
        )
    }) {
        for segment in &out[first_unsafe + 1..] {
            if segment.file_len == 0
                || ctx.discarded_ranges.iter().any(|range| {
                    range.path == segment.path
                        && range.bytes.start == 0
                        && range.bytes.end == segment.file_len
                })
            {
                continue;
            }
            let first_frame = segment.first_valid_frame.or_else(|| {
                ctx.discarded_ranges
                    .iter()
                    .filter(|range| range.path == segment.path)
                    .filter_map(|range| range.first_frame)
                    .min()
            });
            let unknown_final_frame = ctx.discarded_ranges.iter().any(|range| {
                range.path == segment.path
                    && range.bytes.end == segment.file_len
                    && range.last_frame.is_none()
            });
            let last_frame = (!unknown_final_frame)
                .then(|| {
                    std::iter::once(segment.last_valid_frame)
                        .chain(
                            ctx.discarded_ranges
                                .iter()
                                .filter(|range| range.path == segment.path)
                                .flat_map(|range| [range.first_frame, range.last_frame]),
                        )
                        .flatten()
                        .max()
                })
                .flatten();
            ctx.discarded(
                Path::new(&segment.path),
                InspectionByteRange {
                    start: 0,
                    end: segment.file_len,
                },
                first_frame,
                last_frame,
                "wal.after_first_unsafe_range",
            );
        }
    }
    Ok(out)
}

fn validate_wal_published_boundary(
    ctx: &mut InspectionContext,
    published: &InspectedWalPublishedMarker,
    wal_segments: &[InspectedWalSegment],
    persisted_wal_highwater: Option<WalInspectionBoundary>,
) {
    let (Some(marker_segment), Some(marker_frame)) = (published.segment, published.frame) else {
        if !wal_segments.is_empty() {
            ctx.finding(
                "wal.published_missing_or_unreadable",
                InspectionSeverity::Warning,
                Some(Path::new("wal/wal.published")),
                "WAL publication boundary is absent or unreadable",
                None,
            );
        }
        return;
    };

    let marker = WalInspectionBoundary {
        segment: marker_segment,
        frame: marker_frame,
    };
    let boundary_segment = wal_segments
        .iter()
        .find(|segment| segment.segment_id == marker_segment);
    let reset_floor = match (
        published.reset_through_segment,
        published.reset_through_frame,
    ) {
        (Some(segment), Some(frame)) => Some(WalInspectionBoundary { segment, frame }),
        _ => None,
    };
    let effective_floor = persisted_wal_highwater
        .unwrap_or(WalInspectionBoundary {
            segment: 0,
            frame: 0,
        })
        .max(reset_floor.unwrap_or(WalInspectionBoundary {
            segment: 0,
            frame: 0,
        }));
    let marker_reachable = boundary_segment.is_some_and(|segment| {
        (marker_frame == 0 && reset_floor != Some(marker))
            || matches!(
                (segment.first_valid_frame, segment.last_valid_frame),
                (Some(first), Some(last)) if first <= marker_frame && marker_frame <= last
            )
    });
    let required_segment_ids_covered = marker <= effective_floor || {
        let mut required_ids = wal_segments
            .iter()
            .map(|segment| segment.segment_id)
            .filter(|segment_id| {
                *segment_id >= effective_floor.segment && *segment_id <= marker_segment
            });
        required_ids.next().is_some_and(|first| {
            if first != effective_floor.segment {
                return false;
            }
            let mut previous = first;
            for segment_id in required_ids {
                if previous.checked_add(1) != Some(segment_id) {
                    return false;
                }
                previous = segment_id;
            }
            previous == marker_segment
        })
    };
    let absence_covered = effective_floor >= marker
        && boundary_segment.is_some_and(|segment| {
            segment.status == WalSegmentStatus::Clean
                && (segment.file_len == 0
                    || segment
                        .first_valid_frame
                        .is_some_and(|first| first > marker_frame))
        });
    if !required_segment_ids_covered || (!marker_reachable && !absence_covered) {
        ctx.finding(
            "wal.published_boundary_missing",
            InspectionSeverity::Error,
            Some(Path::new("wal/wal.published")),
            format!(
                "published boundary {marker_segment}:{marker_frame} is not backed by the complete \
                 canonical WAL interval from effective floor {}:{}, nor safely absent from a \
                 clean declared segment behind that floor",
                effective_floor.segment, effective_floor.frame
            ),
            None,
        );
    } else if marker_reachable && marker_frame > 0 && published.frame_type != Some(2) {
        ctx.finding(
            "wal.published_not_logical_boundary",
            InspectionSeverity::Error,
            Some(Path::new("wal/wal.published")),
            "nonzero WAL publication marker does not end on a samples-frame commit boundary",
            None,
        );
    } else if marker_reachable && marker_frame == 0 {
        let predecessor_type = wal_segments
            .iter()
            .filter(|segment| segment.segment_id < marker_segment)
            .rev()
            .find_map(|segment| segment.last_valid_frame_type);
        if predecessor_type.is_some() && predecessor_type != Some(2) {
            ctx.finding(
                "wal.published_predecessor_not_logical_boundary",
                InspectionSeverity::Error,
                Some(Path::new("wal/wal.published")),
                "zero-frame publication marker follows a segment that does not end on a samples-frame commit boundary",
                None,
            );
        }
    }
}

type InspectedWalScan = (
    WalSegmentStatus,
    u64,
    Option<u64>,
    Option<u64>,
    Option<u8>,
    Option<u8>,
);

fn inspect_one_wal_segment(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entry: &DiscoveredEntry,
    segment_id: u64,
    published: &mut InspectedWalPublishedMarker,
    previous_sequence: &mut Option<u64>,
) -> Result<InspectedWalScan> {
    let mut file = open_regular_nofollow(source, entry)?;
    let mut offset = 0u64;
    let mut valid_frames = 0u64;
    let mut first_valid = None;
    let mut last_valid = None;
    let mut first_valid_type = None;
    let mut last_valid_type = None;
    let mut status = WalSegmentStatus::Clean;
    let mut unpublished_start = None;
    while offset < entry.len {
        if ctx.work.wal_frames >= ctx.limits.max_wal_frames {
            ctx.hit("max_wal_frames");
            if status == WalSegmentStatus::Clean {
                status = WalSegmentStatus::Incomplete;
            }
            break;
        }
        let remaining = entry.len.saturating_sub(offset);
        if remaining < WAL_FRAME_HEADER_BYTES {
            let range = InspectionByteRange {
                start: offset,
                end: entry.len,
            };
            ctx.finding(
                "wal.corrupt_tail_header",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "WAL ends in a truncated frame header",
                Some(range),
            );
            ctx.discarded(&entry.relative, range, None, None, "wal.corrupt_tail");
            status = WalSegmentStatus::CorruptTail;
            break;
        }
        if !ctx.admit_read(WAL_FRAME_HEADER_BYTES) {
            if status == WalSegmentStatus::Clean {
                status = WalSegmentStatus::Incomplete;
            }
            break;
        }
        let mut header = [0u8; WAL_FRAME_HEADER_BYTES as usize];
        file.read_exact(&mut header)
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry.path.clone(),
                source: source_err,
            })?;
        if header[0..4] != WAL_FRAME_MAGIC {
            let range = InspectionByteRange {
                start: offset,
                end: entry.len,
            };
            ctx.finding(
                "wal.mid_log_magic_mismatch",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "WAL frame magic is invalid; later boundaries are not trustworthy",
                Some(range),
            );
            ctx.discarded(&entry.relative, range, None, None, "wal.mid_log_corruption");
            status = WalSegmentStatus::MidLogCorruption;
            break;
        }
        let frame_type = header[4];
        let frame_seq = read_u64_at(&header, 8);
        let payload_len = u64::from(read_u32_at(&header, 16));
        let expected_crc = read_u32_at(&header, 20);
        let frame_end = offset
            .checked_add(WAL_FRAME_HEADER_BYTES)
            .and_then(|value| value.checked_add(payload_len));
        if payload_len > WAL_MAX_FRAME_PAYLOAD_BYTES || frame_end.is_none() {
            let range = InspectionByteRange {
                start: offset,
                end: entry.len,
            };
            ctx.finding(
                "wal.mid_log_payload_length_invalid",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "WAL frame payload length exceeds the format limit",
                Some(range),
            );
            ctx.discarded(
                &entry.relative,
                range,
                Some(frame_seq),
                None,
                "wal.mid_log_corruption",
            );
            status = WalSegmentStatus::MidLogCorruption;
            break;
        }
        let frame_end = frame_end.expect("checked");
        if frame_end > entry.len {
            let range = InspectionByteRange {
                start: offset,
                end: entry.len,
            };
            ctx.finding(
                "wal.corrupt_tail_payload",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "WAL ends in a truncated frame payload",
                Some(range),
            );
            ctx.discarded(
                &entry.relative,
                range,
                Some(frame_seq),
                Some(frame_seq),
                "wal.corrupt_tail",
            );
            status = WalSegmentStatus::CorruptTail;
            break;
        }
        let header_issue = if header[5..8] != [0, 0, 0] {
            Some((
                "wal.mid_log_reserved_header_invalid",
                "WAL frame reserved header bytes are nonzero",
            ))
        } else if frame_seq == 0
            || previous_sequence.is_some_and(|last| last.checked_add(1) != Some(frame_seq))
        {
            Some((
                "wal.mid_log_sequence_invalid",
                "WAL frame sequence is zero or not globally consecutive",
            ))
        } else if !matches!(frame_type, 1 | 2) {
            Some((
                "wal.mid_log_frame_type_invalid",
                "WAL frame type is unknown",
            ))
        } else {
            None
        };
        if let Some((code, message)) = header_issue {
            let range = InspectionByteRange {
                start: offset,
                end: entry.len,
            };
            ctx.finding(
                code,
                InspectionSeverity::Error,
                Some(&entry.relative),
                message,
                Some(range),
            );
            ctx.discarded(
                &entry.relative,
                range,
                Some(frame_seq),
                None,
                "wal.mid_log_corruption",
            );
            status = WalSegmentStatus::MidLogCorruption;
            break;
        }
        if !ctx.admit_read(payload_len) {
            if status == WalSegmentStatus::Clean {
                status = WalSegmentStatus::Incomplete;
            }
            break;
        }
        if !ctx.admit_hash(payload_len) {
            ctx.work.bytes_read = ctx.work.bytes_read.saturating_sub(payload_len);
            if status == WalSegmentStatus::Clean {
                status = WalSegmentStatus::Incomplete;
            }
            break;
        }
        let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
            TsinkError::DataCorruption(format!(
                "WAL payload length does not fit this platform: {}",
                entry.path.display()
            ))
        })?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_len_usize)
            .map_err(|err| {
                TsinkError::Other(format!(
                    "failed reserving {payload_len_usize} bytes for bounded WAL validation: {err}"
                ))
            })?;
        payload.resize(payload_len_usize, 0);
        let mut crc = crc32fast::Hasher::new();
        let mut left = payload_len;
        let mut payload_offset = 0usize;
        while left > 0 {
            let take =
                usize::try_from(left.min(HASH_BUFFER_BYTES as u64)).unwrap_or(HASH_BUFFER_BYTES);
            let end = payload_offset.saturating_add(take);
            file.read_exact(&mut payload[payload_offset..end])
                .map_err(|source_err| TsinkError::IoWithPath {
                    path: entry.path.clone(),
                    source: source_err,
                })?;
            crc.update(&payload[payload_offset..end]);
            payload_offset = end;
            left -= take as u64;
        }
        let checksum_valid = crc.finalize() == expected_crc;
        if !checksum_valid {
            let range = InspectionByteRange {
                start: offset,
                end: entry.len,
            };
            ctx.finding(
                "wal.mid_log_checksum_mismatch",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "complete WAL frame payload checksum is invalid",
                Some(range),
            );
            ctx.discarded(
                &entry.relative,
                range,
                Some(frame_seq),
                None,
                "wal.mid_log_corruption",
            );
            status = WalSegmentStatus::MidLogCorruption;
            break;
        }
        let decoded_limit = usize::try_from(ctx.limits.max_wal_decoded_bytes).unwrap_or(usize::MAX);
        match crate::engine::wal::validate_frame_payload_for_inspection(
            frame_type,
            &payload,
            decoded_limit,
        ) {
            Ok(true) => {}
            Ok(false) => {
                ctx.hit("max_wal_decoded_bytes");
                if status == WalSegmentStatus::Clean {
                    status = WalSegmentStatus::Incomplete;
                }
                break;
            }
            Err(err) => {
                let range = InspectionByteRange {
                    start: offset,
                    end: entry.len,
                };
                ctx.finding(
                    "wal.mid_log_payload_invalid",
                    InspectionSeverity::Error,
                    Some(&entry.relative),
                    format!("WAL frame payload cannot be strictly decoded: {err}"),
                    Some(range),
                );
                ctx.discarded(
                    &entry.relative,
                    range,
                    Some(frame_seq),
                    None,
                    "wal.mid_log_corruption",
                );
                status = WalSegmentStatus::MidLogCorruption;
                break;
            }
        }
        valid_frames += 1;
        if first_valid.is_none() {
            first_valid = Some(frame_seq);
            first_valid_type = Some(frame_type);
        }
        last_valid = Some(frame_seq);
        last_valid_type = Some(frame_type);
        *previous_sequence = Some(frame_seq);
        if published.segment == Some(segment_id) && published.frame == Some(frame_seq) {
            published.frame_type = Some(frame_type);
        }
        ctx.work.wal_frames += 1;
        let frame_is_unpublished = match (published.segment, published.frame) {
            (Some(marker_segment), Some(marker_frame)) => {
                segment_id > marker_segment
                    || (segment_id == marker_segment && frame_seq > marker_frame)
            }
            _ => false,
        };
        if frame_is_unpublished && unpublished_start.is_none() {
            unpublished_start = Some((offset, frame_seq));
        }
        offset = frame_end;
    }
    if offset == entry.len {
        verify_eof_after_exact_read(&mut file, entry)?;
    }
    verify_file_identity_after_read(&file, source, entry)?;
    if let Some((start, first_frame)) = unpublished_start {
        ctx.discarded(
            &entry.relative,
            InspectionByteRange {
                start,
                end: entry.len,
            },
            Some(first_frame),
            last_valid,
            "wal.unpublished_suffix",
        );
    }
    Ok((
        status,
        valid_frames,
        first_valid,
        last_valid,
        first_valid_type,
        last_valid_type,
    ))
}

fn inspect_persisted_segments(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
) -> Result<(
    Vec<InspectedPersistedSegment>,
    Option<WalInspectionBoundary>,
)> {
    let mut roots = entries
        .iter()
        .filter_map(|entry| {
            (entry.kind == EntryKind::Directory)
                .then(|| parse_segment_root(&entry.relative).map(|identity| (identity, entry)))
                .flatten()
        })
        .collect::<Vec<_>>();
    roots.sort_by_key(|(identity, _)| *identity);
    let mut out = Vec::new();
    let mut persisted_wal_highwater = None;
    for (identity, root) in roots {
        if ctx.work.segments >= ctx.limits.max_segments {
            ctx.hit("max_segments");
            break;
        }
        if !ctx.reserve_report_item() {
            break;
        }
        let Some(path) = ctx.rendered_path(&root.relative) else {
            ctx.cancel_report_item();
            break;
        };
        ctx.work.segments += 1;
        let mut status = PersistedSegmentStatus::Clean;
        let manifest_relative = root.relative.join("manifest.bin");
        let Some(manifest_entry) = find_entry(entries, &manifest_relative) else {
            ctx.finding(
                "segment.manifest_missing",
                InspectionSeverity::Error,
                Some(&manifest_relative),
                "canonical segment is missing manifest.bin",
                None,
            );
            out.push(InspectedPersistedSegment {
                lane: identity.lane.to_string(),
                level: identity.level,
                segment_id: identity.segment_id,
                path,
                manifest_version: None,
                manifest_checksum_valid: None,
                status: PersistedSegmentStatus::MissingFiles,
            });
            continue;
        };
        if manifest_entry.kind != EntryKind::File {
            ctx.finding(
                "segment.manifest_not_regular",
                InspectionSeverity::Error,
                Some(&manifest_entry.relative),
                "segment manifest is not a regular non-link file",
                None,
            );
            out.push(InspectedPersistedSegment {
                lane: identity.lane.to_string(),
                level: identity.level,
                segment_id: identity.segment_id,
                path,
                manifest_version: None,
                manifest_checksum_valid: None,
                status: PersistedSegmentStatus::Corrupt,
            });
            continue;
        }
        if manifest_entry.len > 1024 * 1024 {
            ctx.finding(
                "segment.manifest_oversized",
                InspectionSeverity::Error,
                Some(&manifest_entry.relative),
                "segment manifest exceeds its 1 MiB format limit",
                None,
            );
            out.push(InspectedPersistedSegment {
                lane: identity.lane.to_string(),
                level: identity.level,
                segment_id: identity.segment_id,
                path,
                manifest_version: None,
                manifest_checksum_valid: None,
                status: PersistedSegmentStatus::Corrupt,
            });
            continue;
        }
        let Some(bytes) = read_regular_file(ctx, source, manifest_entry, manifest_entry.len)?
        else {
            out.push(InspectedPersistedSegment {
                lane: identity.lane.to_string(),
                level: identity.level,
                segment_id: identity.segment_id,
                path,
                manifest_version: None,
                manifest_checksum_valid: None,
                status: PersistedSegmentStatus::Incomplete,
            });
            continue;
        };
        let manifest_checksum_valid = if bytes.len() >= 4 {
            let hashed = u64::try_from(bytes.len() - 4).unwrap_or(u64::MAX);
            if !ctx.admit_hash(hashed) {
                out.push(InspectedPersistedSegment {
                    lane: identity.lane.to_string(),
                    level: identity.level,
                    segment_id: identity.segment_id,
                    path,
                    manifest_version: bytes
                        .get(4..6)
                        .map(|raw| u16::from_le_bytes([raw[0], raw[1]])),
                    manifest_checksum_valid: None,
                    status: PersistedSegmentStatus::Incomplete,
                });
                continue;
            }
            Some(crc32fast::hash(&bytes[..bytes.len() - 4]) == read_u32_at(&bytes, bytes.len() - 4))
        } else {
            None
        };
        let parsed = parse_segment_manifest(&bytes, manifest_checksum_valid);
        let parsed = match parsed {
            Ok(parsed) => parsed,
            Err(reason) => {
                ctx.finding(
                    "segment.manifest_corrupt",
                    InspectionSeverity::Error,
                    Some(&manifest_entry.relative),
                    reason,
                    None,
                );
                out.push(InspectedPersistedSegment {
                    lane: identity.lane.to_string(),
                    level: identity.level,
                    segment_id: identity.segment_id,
                    path,
                    manifest_version: bytes
                        .get(4..6)
                        .map(|raw| u16::from_le_bytes([raw[0], raw[1]])),
                    manifest_checksum_valid,
                    status: PersistedSegmentStatus::Corrupt,
                });
                continue;
            }
        };
        if parsed.segment_id != identity.segment_id || parsed.level != identity.level {
            ctx.finding(
                "segment.identity_mismatch",
                InspectionSeverity::Error,
                Some(&manifest_entry.relative),
                "manifest segment identity does not match its canonical path",
                None,
            );
            status = PersistedSegmentStatus::Corrupt;
        }
        for expectation in parsed.files {
            let file_name = match expectation.kind {
                1 => "chunks.bin",
                2 => "chunk_index.bin",
                3 => "series.bin",
                4 => "postings.bin",
                _ => {
                    ctx.finding(
                        "segment.manifest_file_kind_invalid",
                        InspectionSeverity::Error,
                        Some(&manifest_entry.relative),
                        format!("manifest references unknown file kind {}", expectation.kind),
                        None,
                    );
                    status = PersistedSegmentStatus::Corrupt;
                    continue;
                }
            };
            let format_ceiling = if expectation.kind == 1 {
                crate::engine::segment::MAX_SEGMENT_CHUNKS_FILE_BYTES as u64
            } else {
                crate::engine::binio::MAX_DECODED_FRAMED_FILE_BYTES as u64
            };
            if expectation.len > format_ceiling {
                ctx.finding(
                    "segment.file_exceeds_format_limit",
                    InspectionSeverity::Error,
                    Some(&root.relative.join(file_name)),
                    format!(
                        "segment manifest length {} exceeds format ceiling {format_ceiling}",
                        expectation.len
                    ),
                    None,
                );
                status = PersistedSegmentStatus::Corrupt;
                continue;
            }
            let relative = root.relative.join(file_name);
            let Some(file_entry) = find_entry(entries, &relative) else {
                ctx.finding(
                    "segment.referenced_file_missing",
                    InspectionSeverity::Error,
                    Some(&relative),
                    "segment manifest references a missing file",
                    None,
                );
                status = PersistedSegmentStatus::MissingFiles;
                continue;
            };
            if file_entry.kind != EntryKind::File {
                ctx.finding(
                    "segment.referenced_file_not_regular",
                    InspectionSeverity::Error,
                    Some(&file_entry.relative),
                    "segment file is not a regular non-link file",
                    None,
                );
                status = PersistedSegmentStatus::Corrupt;
                continue;
            }
            if file_entry.len != expectation.len {
                ctx.finding(
                    "segment.file_length_mismatch",
                    InspectionSeverity::Error,
                    Some(&file_entry.relative),
                    format!(
                        "manifest length {} differs from physical length {}",
                        expectation.len, file_entry.len
                    ),
                    None,
                );
                status = PersistedSegmentStatus::Corrupt;
                continue;
            }
            match hash_regular_file(ctx, source, file_entry)? {
                Some(actual) if actual == expectation.hash64 => {}
                Some(_) => {
                    ctx.finding(
                        "segment.file_checksum_mismatch",
                        InspectionSeverity::Error,
                        Some(&file_entry.relative),
                        "segment file xxh64 does not match manifest",
                        None,
                    );
                    status = PersistedSegmentStatus::Corrupt;
                }
                None => status = PersistedSegmentStatus::Incomplete,
            }
        }
        if status == PersistedSegmentStatus::Clean {
            persisted_wal_highwater = Some(
                persisted_wal_highwater
                    .map_or(parsed.wal_highwater, |current: WalInspectionBoundary| {
                        current.max(parsed.wal_highwater)
                    }),
            );
        }
        out.push(InspectedPersistedSegment {
            lane: identity.lane.to_string(),
            level: identity.level,
            segment_id: identity.segment_id,
            path,
            manifest_version: Some(parsed.version),
            manifest_checksum_valid: Some(parsed.checksum_valid),
            status,
        });
    }
    Ok((out, persisted_wal_highwater))
}

fn validate_catalog_references(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entries: &[DiscoveredEntry],
    segments: &[InspectedPersistedSegment],
) -> Result<()> {
    let actual = segments
        .iter()
        .map(|segment| SegmentIdentity {
            lane: if segment.lane == "numeric" {
                "numeric"
            } else {
                "blob"
            },
            level: segment.level,
            segment_id: segment.segment_id,
        })
        .collect::<BTreeSet<_>>();
    let mut referenced = BTreeSet::new();
    let mut saw_complete_catalog = false;
    if let Some(entry) = find_entry(entries, Path::new("series_index.catalog.json")) {
        if let Some(bytes) = read_catalog_bytes(ctx, source, entry)? {
            if !catalog_decode_preflight(ctx, entry, &bytes) {
                return Ok(());
            }
            match serde_json::from_slice::<LocalCatalog>(&bytes) {
                Ok(catalog) if catalog.version == 2 => {
                    saw_complete_catalog = true;
                    collect_catalog_refs(
                        ctx,
                        &entry.relative,
                        catalog.segments,
                        false,
                        &mut referenced,
                    );
                }
                Ok(catalog) => ctx.finding(
                    "catalog.local_version_unsupported",
                    InspectionSeverity::Warning,
                    Some(&entry.relative),
                    format!("local catalog version {} is unsupported", catalog.version),
                    None,
                ),
                Err(err) => ctx.finding(
                    "catalog.local_corrupt",
                    InspectionSeverity::Error,
                    Some(&entry.relative),
                    format!("local catalog JSON cannot be decoded: {err}"),
                    None,
                ),
            }
        }
    }
    if let Some(entry) = find_entry(entries, Path::new("segment_catalog.json")) {
        if let Some(bytes) = read_catalog_bytes(ctx, source, entry)? {
            if !catalog_decode_preflight(ctx, entry, &bytes) {
                return Ok(());
            }
            match serde_json::from_slice::<RemoteCatalog>(&bytes) {
                Ok(catalog) if catalog.version == 2 => {
                    saw_complete_catalog = true;
                    collect_catalog_refs(
                        ctx,
                        &entry.relative,
                        catalog.entries,
                        true,
                        &mut referenced,
                    );
                }
                Ok(catalog) => ctx.finding(
                    "catalog.remote_version_unsupported",
                    InspectionSeverity::Warning,
                    Some(&entry.relative),
                    format!("remote catalog version {} is unsupported", catalog.version),
                    None,
                ),
                Err(err) => ctx.finding(
                    "catalog.remote_corrupt",
                    InspectionSeverity::Error,
                    Some(&entry.relative),
                    format!("remote catalog JSON cannot be decoded: {err}"),
                    None,
                ),
            }
        }
    }
    // The incremental local store is exact when its manifest says complete. Its canonical file
    // names carry the complete identity needed for missing/orphan detection.
    let store_manifest_relative = Path::new("series_index.catalog.d").join("manifest.json");
    if let Some(entry) = find_entry(entries, &store_manifest_relative) {
        if let Some(bytes) = read_catalog_bytes(ctx, source, entry)? {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct StoreManifest {
                version: u32,
                complete: bool,
                entry_count: usize,
                #[serde(default)]
                series_fingerprint: Option<StoreFingerprint>,
                #[serde(default)]
                pending_delta: Option<serde::de::IgnoredAny>,
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct StoreFingerprint {
                series_count: usize,
                series_hash64: u64,
            }
            match serde_json::from_slice::<StoreManifest>(&bytes) {
                Ok(manifest)
                    if manifest.version == 1
                        && manifest.complete
                        && manifest.pending_delta.is_none()
                        && (manifest.entry_count != 0
                            || manifest.series_fingerprint.as_ref().is_none_or(
                                |fingerprint| {
                                    fingerprint.series_count == 0
                                        && fingerprint.series_hash64
                                            == xxhash_rust::xxh64::xxh64(&[], 0)
                                },
                            )) =>
                {
                    let mut observed = 0usize;
                    for candidate in entries.iter().filter(|candidate| {
                        candidate.relative.parent() == Some(Path::new("series_index.catalog.d"))
                            && candidate.kind == EntryKind::File
                    }) {
                        let Some(name) = candidate
                            .relative
                            .file_name()
                            .and_then(|name| name.to_str())
                        else {
                            continue;
                        };
                        if let Some(identity) = parse_registry_catalog_entry_name(name) {
                            observed = observed.saturating_add(1);
                            referenced.insert(identity);
                        }
                    }
                    if observed != manifest.entry_count {
                        ctx.finding(
                            "catalog.local_store_entry_count_mismatch",
                            InspectionSeverity::Error,
                            Some(&entry.relative),
                            format!(
                                "catalog manifest declares {} entries but {} canonical entries were found",
                                manifest.entry_count, observed
                            ),
                            None,
                        );
                    } else {
                        saw_complete_catalog = true;
                    }
                }
                Ok(_) => ctx.finding(
                    "catalog.local_store_incomplete",
                    InspectionSeverity::Warning,
                    Some(&entry.relative),
                    "incremental local catalog is unsupported or not complete",
                    None,
                ),
                Err(err) => ctx.finding(
                    "catalog.local_store_manifest_corrupt",
                    InspectionSeverity::Error,
                    Some(&entry.relative),
                    format!("incremental local catalog manifest cannot be decoded: {err}"),
                    None,
                ),
            }
        }
    } else if find_entry(entries, Path::new("series_index.catalog.d")).is_some() {
        ctx.finding(
            "catalog.local_store_manifest_missing",
            InspectionSeverity::Error,
            Some(&store_manifest_relative),
            "incremental local catalog directory is missing manifest.json",
            None,
        );
    }
    if saw_complete_catalog {
        for missing in referenced.difference(&actual) {
            ctx.finding(
                "catalog.referenced_segment_missing",
                InspectionSeverity::Error,
                None,
                format!(
                    "catalog references missing {} segment L{}:{:016x}",
                    missing.lane, missing.level, missing.segment_id
                ),
                None,
            );
        }
        for orphan in actual.difference(&referenced) {
            ctx.finding(
                "catalog.unreferenced_segment",
                InspectionSeverity::Warning,
                None,
                format!(
                    "canonical {} segment L{}:{:016x} is not referenced by a complete catalog",
                    orphan.lane, orphan.level, orphan.segment_id
                ),
                None,
            );
        }
    }
    Ok(())
}

fn catalog_decode_preflight(
    ctx: &mut InspectionContext,
    entry: &DiscoveredEntry,
    bytes: &[u8],
) -> bool {
    let preflight = preflight_json_collections(
        bytes,
        ctx.limits.max_segments,
        256,
        ctx.limits.max_catalog_bytes,
    );
    let stats = match preflight {
        Ok(stats) => stats,
        Err(PersistedReportPreflightError::ReportItems) => {
            ctx.hit("max_segments");
            ctx.finding(
                "catalog.decode_limit_exceeded",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "catalog reference array exceeds max_segments before typed decoding",
                None,
            );
            return false;
        }
        Err(_) => {
            ctx.hit("max_catalog_bytes");
            ctx.finding(
                "catalog.decode_limit_exceeded",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "catalog strings, escaping, or nesting exceed bounded decode limits",
                None,
            );
            return false;
        }
    };
    let modeled = u64::try_from(bytes.len())
        .unwrap_or(u64::MAX)
        .checked_add(stats.string_bytes)
        .and_then(|bytes| {
            stats
                .array_items
                .checked_mul(
                    u64::try_from(std::mem::size_of::<CatalogReference>()).unwrap_or(u64::MAX),
                )
                .and_then(|items| bytes.checked_add(items))
        });
    let modeled_within_limit = modeled.is_some_and(|bytes| bytes <= ctx.limits.max_catalog_bytes);
    if !modeled_within_limit {
        ctx.hit("max_catalog_bytes");
        ctx.finding(
            "catalog.decode_limit_exceeded",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "catalog modeled decoded memory exceeds max_catalog_bytes",
            None,
        );
        return false;
    }
    true
}

fn collect_catalog_refs(
    ctx: &mut InspectionContext,
    relative: &Path,
    references: Vec<CatalogReference>,
    remote: bool,
    out: &mut BTreeSet<SegmentIdentity>,
) {
    if references.len() as u64 > ctx.limits.max_segments {
        ctx.hit("max_segments");
        return;
    }
    for reference in references {
        if remote
            && reference
                .tier
                .as_deref()
                .is_some_and(|tier| !matches!(tier, "hot"))
        {
            ctx.finding(
                "catalog.external_reference_unresolved",
                InspectionSeverity::Info,
                Some(relative),
                "warm/cold catalog reference requires the separately configured object-store root",
                None,
            );
            continue;
        }
        let lane = match reference.lane.as_str() {
            "numeric" => "numeric",
            "blob" => "blob",
            _ => {
                ctx.finding(
                    "catalog.reference_lane_invalid",
                    InspectionSeverity::Error,
                    Some(relative),
                    "catalog reference has an unknown lane",
                    None,
                );
                continue;
            }
        };
        out.insert(SegmentIdentity {
            lane,
            level: reference.level,
            segment_id: reference.segment_id,
        });
    }
}

fn read_catalog_bytes(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entry: &DiscoveredEntry,
) -> Result<Option<Vec<u8>>> {
    if entry.kind != EntryKind::File {
        ctx.finding(
            "catalog.not_regular",
            InspectionSeverity::Error,
            Some(&entry.relative),
            "catalog is not a regular non-link file",
            None,
        );
        return Ok(None);
    }
    if entry.len > ctx.limits.max_catalog_bytes {
        ctx.finding(
            "catalog.file_too_large",
            InspectionSeverity::Error,
            Some(&entry.relative),
            format!(
                "catalog length {} exceeds max_catalog_bytes {}",
                entry.len, ctx.limits.max_catalog_bytes
            ),
            None,
        );
        ctx.hit("max_catalog_bytes");
        return Ok(None);
    }
    read_regular_file(ctx, source, entry, entry.len)
}

fn report_unknown_namespace(ctx: &mut InspectionContext, entries: &[DiscoveredEntry]) {
    for entry in entries {
        if utf8_path_components(&entry.relative).is_none() {
            ctx.finding(
                "namespace.opaque_path_component",
                InspectionSeverity::Warning,
                Some(&entry.relative),
                "path contains a non-UTF-8 component and cannot match the canonical namespace",
                None,
            );
        }
        if let Some(expected) = expected_known_path_kind(&entry.relative) {
            if entry.kind != expected {
                ctx.finding(
                    "namespace.known_path_kind_invalid",
                    InspectionSeverity::Error,
                    Some(&entry.relative),
                    format!(
                        "recognized path has kind {:?}, expected {:?}",
                        entry.kind, expected
                    ),
                    None,
                );
            }
        }
        if !is_known_namespace_path(&entry.relative, entry.kind) {
            ctx.finding(
                "namespace.unknown_path",
                InspectionSeverity::Warning,
                Some(&entry.relative),
                "path is outside the recognized current or supported legacy namespace",
                None,
            );
        }
        if parse_segment_root(&entry.relative).is_some() && entry.kind != EntryKind::Directory {
            ctx.finding(
                "segment.root_not_directory",
                InspectionSeverity::Error,
                Some(&entry.relative),
                "canonical segment root is not a real directory",
                None,
            );
        }
        if let Some(parent) = entry.relative.parent() {
            if parse_segment_root(parent).is_some()
                && !matches!(
                    entry.relative.file_name().and_then(|name| name.to_str()),
                    Some(
                        "manifest.bin"
                            | "chunks.bin"
                            | "chunk_index.bin"
                            | "series.bin"
                            | "postings.bin"
                    )
                )
            {
                ctx.finding(
                    "segment.unexpected_file",
                    InspectionSeverity::Warning,
                    Some(&entry.relative),
                    "unexpected object exists inside a canonical segment directory",
                    None,
                );
            }
        }
    }
}

fn utf8_path_components(path: &Path) -> Option<Vec<&str>> {
    path.components()
        .map(|component| component.as_os_str().to_str())
        .collect()
}

fn expected_known_path_kind(relative: &Path) -> Option<EntryKind> {
    let components = utf8_path_components(relative)?;
    let first = *components.first()?;
    if components.len() == 1 {
        return if matches!(
            first,
            ".tsink.lock"
                | DATA_DIRECTORY_MANIFEST_FILE_NAME
                | SALVAGE_REPORT_FILE_NAME
                | "series_index.bin"
                | "series_index.delta.bin"
                | "series_index.catalog.json"
                | "segment_catalog.json"
                | "metric-metadata-store.json"
                | "exemplar-store.json"
                | "rules-store.json"
        ) {
            Some(EntryKind::File)
        } else if matches!(
            first,
            "lane_numeric"
                | "lane_blob"
                | "wal"
                | "series_index.delta.d"
                | "series_index.catalog.d"
                | ".tombstone-transactions"
                | ".post-flush-replacements"
                | ".rollups"
                | "usage-accounting"
                | "managed-control-plane"
                | "edge_sync"
                | "cluster"
        ) {
            Some(EntryKind::Directory)
        } else {
            None
        };
    }
    if first == "wal" && components.len() == 2 {
        return Some(EntryKind::File);
    }
    if matches!(first, "lane_numeric" | "lane_blob") {
        if components.len() == 2 {
            return match components[1] {
                "segments" | "tombstones.json.store" => Some(EntryKind::Directory),
                "tombstones.json" => Some(EntryKind::File),
                name if is_exact_tombstone_atomic_temp(name) => Some(EntryKind::File),
                _ => None,
            };
        }
        if components.get(1) == Some(&"segments") {
            return match components.len() {
                3 if parse_level(components[2]).is_some() => Some(EntryKind::Directory),
                4 if parse_level(components[2]).is_some()
                    && parse_exact_segment_dir(components[3]).is_some() =>
                {
                    Some(EntryKind::Directory)
                }
                5 if parse_level(components[2]).is_some()
                    && parse_exact_segment_dir(components[3]).is_some()
                    && matches!(
                        components[4],
                        "manifest.bin"
                            | "chunks.bin"
                            | "chunk_index.bin"
                            | "series.bin"
                            | "postings.bin"
                    ) =>
                {
                    Some(EntryKind::File)
                }
                _ => None,
            };
        }
        if components.get(1) == Some(&"tombstones.json.store") {
            return match components.as_slice() {
                [_, _, "shards"] => Some(EntryKind::Directory),
                [_, _, "shards", shard] if is_exact_tombstone_shard_name(shard) => {
                    Some(EntryKind::File)
                }
                _ => None,
            };
        }
    }
    if first == "series_index.catalog.d" && components.len() == 2 {
        return Some(EntryKind::File);
    }
    None
}

fn report_unverified_recovery_objects(ctx: &mut InspectionContext, entries: &[DiscoveredEntry]) {
    for entry in entries {
        if is_unverified_recovery_object(&entry.relative) {
            ctx.finding(
                "namespace.recovery_object_unverified",
                InspectionSeverity::Warning,
                Some(&entry.relative),
                "recognized recovery-critical object has no bounded format validator and cannot be certified clean",
                None,
            );
        }
    }
}

fn report_unverified_cross_artifact_state(
    ctx: &mut InspectionContext,
    registry: &InspectedSeriesRegistry,
    wal_segments: &[InspectedWalSegment],
    segments: &[InspectedPersistedSegment],
) {
    for segment in wal_segments
        .iter()
        .filter(|segment| segment.valid_frames > 0)
    {
        ctx.finding(
            "wal.production_replay_semantics_unverified",
            InspectionSeverity::Warning,
            Some(Path::new(&segment.path)),
            "WAL frame payloads decode independently, but production replay identity, lane, and transaction semantics were not reconstructed",
            None,
        );
    }
    for segment in segments {
        ctx.finding(
            "segment.production_structure_unverified",
            InspectionSeverity::Warning,
            Some(Path::new(&segment.path)),
            "segment checksums and lengths were checked independently, but production chunk/index/series/postings decoding was not completed",
            None,
        );
    }
    if registry.series_count.unwrap_or(0) > 0
        && (wal_segments.iter().any(|segment| segment.valid_frames > 0) || !segments.is_empty())
    {
        ctx.finding(
            "identity.cross_artifact_consistency_unverified",
            InspectionSeverity::Warning,
            Some(Path::new("series_index.bin")),
            "registry, WAL, and persisted-segment identities were validated independently but not cross-compared",
            None,
        );
    }
}

fn is_unverified_recovery_object(relative: &Path) -> bool {
    let Some(components) = utf8_path_components(relative) else {
        return false;
    };
    let Some(first) = components.first().copied() else {
        return false;
    };
    if matches!(
        first,
        "series_index.catalog.json"
            | "segment_catalog.json"
            | "series_index.delta.bin"
            | "series_index.delta.d"
            | "metric-metadata-store.json"
            | "exemplar-store.json"
            | "rules-store.json"
            | ".tombstone-transactions"
            | ".post-flush-replacements"
            | ".rollups"
            | "usage-accounting"
            | "managed-control-plane"
            | "edge_sync"
            | "cluster"
    ) {
        return true;
    }
    if first == "wal" {
        return components.get(1) == Some(&"wal.published.tmp");
    }
    if first == "series_index.catalog.d" {
        return components.len() >= 2 && components.get(1) != Some(&"manifest.json");
    }
    matches!(first, "lane_numeric" | "lane_blob")
        && components.get(1).is_some_and(|name| {
            name.starts_with("tombstones.json") || name.starts_with(".tombstones.json")
        })
}

fn is_known_namespace_path(relative: &Path, kind: EntryKind) -> bool {
    let Some(components) = utf8_path_components(relative) else {
        return false;
    };
    let Some(first) = components.first().copied() else {
        return true;
    };
    if components.len() == 1 {
        return matches!(
            first,
            ".tsink.lock"
                | DATA_DIRECTORY_MANIFEST_FILE_NAME
                | SALVAGE_REPORT_FILE_NAME
                | "series_index.bin"
                | "series_index.delta.bin"
                | "series_index.catalog.json"
                | "segment_catalog.json"
                | "metric-metadata-store.json"
                | "exemplar-store.json"
                | "rules-store.json"
                | "lane_numeric"
                | "lane_blob"
                | "wal"
                | "series_index.delta.d"
                | "series_index.catalog.d"
                | ".tombstone-transactions"
                | ".post-flush-replacements"
                | ".rollups"
                | "usage-accounting"
                | "managed-control-plane"
                | "edge_sync"
                | "cluster"
        );
    }
    if first == "wal" {
        return components.len() == 2
            && kind == EntryKind::File
            && (matches!(
                components[1],
                "wal.log" | "wal.published" | "wal.published.tmp"
            ) || parse_exact_hex_file(components[1], "wal-", ".log").is_some());
    }
    if matches!(first, "lane_numeric" | "lane_blob") {
        if components.len() == 2 {
            return match components[1] {
                "segments" | "tombstones.json.store" => kind == EntryKind::Directory,
                "tombstones.json" => kind == EntryKind::File,
                name => kind == EntryKind::File && is_exact_tombstone_atomic_temp(name),
            };
        }
        if components.get(1) == Some(&"segments") {
            if components.len() == 3 {
                return parse_level(components[2]).is_some();
            }
            if components.len() == 4 {
                return parse_level(components[2]).is_some()
                    && parse_exact_segment_dir(components[3]).is_some();
            }
            if components.len() == 5 {
                return parse_level(components[2]).is_some()
                    && parse_exact_segment_dir(components[3]).is_some()
                    && matches!(
                        components[4],
                        "manifest.bin"
                            | "chunks.bin"
                            | "chunk_index.bin"
                            | "series.bin"
                            | "postings.bin"
                    );
            }
            return false;
        }
        if components.get(1) == Some(&"tombstones.json.store") {
            if components.len() == 3 {
                return components[2] == "shards" && kind == EntryKind::Directory;
            }
            if components.len() == 4 && components[2] == "shards" {
                return kind == EntryKind::File && is_exact_tombstone_shard_name(components[3]);
            }
        }
        return false;
    }
    if first == "series_index.catalog.d" {
        return components.len() == 2
            && (components[1] == "manifest.json"
                || parse_registry_catalog_entry_name(components[1]).is_some()
                || components[1].starts_with('.'));
    }
    // Recognized auxiliary namespaces are traversed for bounds/link safety but their internal
    // formats are owned by separate inspectors.
    matches!(
        first,
        "series_index.delta.d"
            | ".tombstone-transactions"
            | ".post-flush-replacements"
            | ".rollups"
            | "usage-accounting"
            | "managed-control-plane"
            | "edge_sync"
            | "cluster"
    )
}

fn is_exact_tombstone_atomic_temp(name: &str) -> bool {
    let Some(suffix) = name
        .strip_prefix(".tombstones.json.tmp-")
        .and_then(|suffix| suffix.split_once('-'))
    else {
        return false;
    };
    let (pid, nonce) = suffix;
    pid.parse::<u32>()
        .ok()
        .is_some_and(|value| value.to_string() == pid)
        && parse_exact_lower_hex_16(nonce).is_some()
}

fn is_exact_tombstone_shard_name(name: &str) -> bool {
    let Some(encoded) = name
        .strip_prefix("shard-")
        .and_then(|encoded| encoded.strip_suffix(".bin"))
    else {
        return false;
    };
    let Some((shard, nonce)) = encoded.split_once('-') else {
        return false;
    };
    shard.len() == 3
        && shard.bytes().all(|byte| byte.is_ascii_digit())
        && shard
            .parse::<u16>()
            .ok()
            .is_some_and(|value| value < 256 && format!("{value:03}") == shard)
        && parse_exact_lower_hex_16(nonce).is_some()
}

fn parse_segment_root(path: &Path) -> Option<SegmentIdentity> {
    let components = utf8_path_components(path)?;
    if components.len() != 4 || components[1] != "segments" {
        return None;
    }
    let lane = match components[0] {
        "lane_numeric" => "numeric",
        "lane_blob" => "blob",
        _ => return None,
    };
    Some(SegmentIdentity {
        lane,
        level: parse_level(components[2])?,
        segment_id: parse_exact_segment_dir(components[3])?,
    })
}

fn parse_level(name: &str) -> Option<u8> {
    let encoded = name.strip_prefix('L')?;
    let level = encoded.parse::<u8>().ok()?;
    (level <= 2 && level.to_string() == encoded).then_some(level)
}

fn parse_exact_segment_dir(name: &str) -> Option<u64> {
    let encoded = name.strip_prefix("seg-")?;
    parse_exact_lower_hex_16(encoded)
}

fn parse_exact_hex_file(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    let encoded = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    parse_exact_lower_hex_16(encoded)
}

fn parse_exact_lower_hex_16(encoded: &str) -> Option<u64> {
    if encoded.len() != 16
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn parse_registry_catalog_entry_name(name: &str) -> Option<SegmentIdentity> {
    let encoded = name.strip_prefix("segment-")?.strip_suffix(".json")?;
    let (lane, remainder) = encoded.split_once('-')?;
    let lane = match lane {
        "numeric" => "numeric",
        "blob" => "blob",
        _ => return None,
    };
    let (level, segment_id) = remainder.split_once('-')?;
    if level.len() != 2
        || !level
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || segment_id.len() != 16
        || !segment_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let level_value = u8::from_str_radix(level, 16).ok()?;
    let segment_id_value = u64::from_str_radix(segment_id, 16).ok()?;
    if format!("{level_value:02x}") != level || format!("{segment_id_value:016x}") != segment_id {
        return None;
    }
    Some(SegmentIdentity {
        lane,
        level: level_value,
        segment_id: segment_id_value,
    })
}

fn parse_segment_manifest(
    bytes: &[u8],
    checksum_valid: Option<bool>,
) -> std::result::Result<ParsedSegmentManifest, String> {
    if bytes.len() != SEGMENT_MANIFEST_BYTES {
        return Err(format!(
            "manifest.bin must be exactly {SEGMENT_MANIFEST_BYTES} bytes, found {}",
            bytes.len()
        ));
    }
    if bytes[0..4] != SEGMENT_MANIFEST_MAGIC {
        return Err("manifest.bin magic mismatch".to_string());
    }
    let version = read_u16_at(bytes, 4);
    if version != SEGMENT_FORMAT_VERSION {
        return Err(format!("unsupported segment manifest version {version}"));
    }
    let checksum_valid = checksum_valid.unwrap_or(false);
    if !checksum_valid {
        return Err("manifest.bin CRC32 mismatch".to_string());
    }
    let segment_id = read_u64_at(bytes, 8);
    let level = bytes[16];
    let wal_highwater = WalInspectionBoundary {
        segment: read_u64_at(bytes, 72),
        frame: read_u64_at(bytes, 80),
    };
    let entry_count = read_u32_at(bytes, 88);
    if entry_count != 4 {
        return Err(format!(
            "manifest.bin declares {entry_count} file entries instead of 4"
        ));
    }
    let mut files = [SegmentFileExpectation {
        kind: 0,
        len: 0,
        hash64: 0,
    }; 4];
    let mut seen = BTreeSet::new();
    for (index, output) in files.iter_mut().enumerate() {
        let offset = 96 + index * 20;
        *output = SegmentFileExpectation {
            kind: bytes[offset],
            len: read_u64_at(bytes, offset + 4),
            hash64: read_u64_at(bytes, offset + 12),
        };
        if !seen.insert(output.kind) {
            return Err("manifest.bin contains duplicate file kinds".to_string());
        }
    }
    Ok(ParsedSegmentManifest {
        version,
        segment_id,
        level,
        wal_highwater,
        checksum_valid,
        files,
    })
}

fn read_regular_file(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entry: &DiscoveredEntry,
    max_len: u64,
) -> Result<Option<Vec<u8>>> {
    if entry.len > max_len {
        ctx.finding(
            "inspection.file_exceeds_operation_limit",
            InspectionSeverity::Error,
            Some(&entry.relative),
            format!(
                "file length {} exceeds this operation's limit {max_len}",
                entry.len
            ),
            None,
        );
        return Ok(None);
    }
    if !ctx.admit_read(entry.len) {
        return Ok(None);
    }
    let mut file = open_regular_nofollow(source, entry)?;
    let len = usize::try_from(entry.len).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "file length does not fit this platform: {}",
            entry.path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(|err| {
        TsinkError::Other(format!(
            "failed reserving {len} bytes for bounded inspection read: {err}"
        ))
    })?;
    bytes.resize(len, 0);
    file.read_exact(&mut bytes)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: entry.path.clone(),
            source: source_err,
        })?;
    verify_eof_after_exact_read(&mut file, entry)?;
    if bytes.len() != len {
        return Err(TsinkError::DataCorruption(format!(
            "file length changed while reading: {}",
            entry.path.display()
        )));
    }
    verify_file_identity_after_read(&file, source, entry)?;
    Ok(Some(bytes))
}

fn hash_regular_file(
    ctx: &mut InspectionContext,
    source: &InspectionSource<'_>,
    entry: &DiscoveredEntry,
) -> Result<Option<u64>> {
    if !ctx.admit_hash(entry.len) {
        return Ok(None);
    }
    if !ctx.admit_read(entry.len) {
        // Keep work truthful if the paired read could not be admitted.
        ctx.work.bytes_hashed = ctx.work.bytes_hashed.saturating_sub(entry.len);
        return Ok(None);
    }
    let mut file = open_regular_nofollow(source, entry)?;
    let mut hasher = Xxh64::new(0);
    let mut read_total = 0u64;
    let mut remaining = entry.len;
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    while remaining > 0 {
        let take = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        file.read_exact(&mut buffer[..take])
            .map_err(|source_err| TsinkError::IoWithPath {
                path: entry.path.clone(),
                source: source_err,
            })?;
        hasher.write(&buffer[..take]);
        read_total = read_total.saturating_add(take as u64);
        remaining -= take as u64;
    }
    if read_total != entry.len {
        return Err(TsinkError::DataCorruption(format!(
            "file length changed while hashing: {}",
            entry.path.display()
        )));
    }
    verify_eof_after_exact_read(&mut file, entry)?;
    verify_file_identity_after_read(&file, source, entry)?;
    Ok(Some(hasher.finish()))
}

fn verify_eof_after_exact_read(file: &mut File, entry: &DiscoveredEntry) -> Result<()> {
    let mut probe = [0u8; 1];
    let read = file
        .read(&mut probe)
        .map_err(|source_err| TsinkError::IoWithPath {
            path: entry.path.clone(),
            source: source_err,
        })?;
    if read != 0 {
        return Err(TsinkError::DataCorruption(format!(
            "file grew beyond its admitted length during inspection: {}",
            entry.path.display()
        )));
    }
    Ok(())
}

fn open_regular_nofollow(source: &InspectionSource<'_>, entry: &DiscoveredEntry) -> Result<File> {
    if entry.kind != EntryKind::File {
        return Err(TsinkError::DataCorruption(format!(
            "refusing to inspect non-regular or escaped path: {}",
            entry.path.display()
        )));
    }
    let file = open_relative_regular_file_nofollow(source.root, &entry.relative, &entry.path)?;
    verify_opened_identity(
        &file,
        EntryStat {
            kind: entry.kind,
            len: entry.len,
            identity: entry.identity,
        },
        &entry.path,
    )?;
    Ok(file)
}

fn verify_file_identity_after_read(
    file: &File,
    source: &InspectionSource<'_>,
    entry: &DiscoveredEntry,
) -> Result<()> {
    let opened_metadata = file
        .metadata()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: entry.path.clone(),
            source: source_err,
        })?;
    if !opened_metadata.file_type().is_file()
        || opened_metadata.len() != entry.len
        || metadata_identity(&opened_metadata) != entry.identity
    {
        return Err(TsinkError::DataCorruption(format!(
            "opened file identity or length changed during bounded inspection: {}",
            entry.path.display()
        )));
    }
    let opened_file = file
        .try_clone()
        .map_err(|source_err| TsinkError::IoWithPath {
            path: entry.path.clone(),
            source: source_err,
        })?;
    let opened =
        same_file::Handle::from_file(opened_file).map_err(|source_err| TsinkError::IoWithPath {
            path: entry.path.clone(),
            source: source_err,
        })?;
    let current_file =
        open_relative_regular_file_nofollow(source.root, &entry.relative, &entry.path)?;
    verify_opened_identity(
        &current_file,
        EntryStat {
            kind: entry.kind,
            len: entry.len,
            identity: entry.identity,
        },
        &entry.path,
    )?;
    let current = same_file::Handle::from_file(current_file).map_err(|source_err| {
        TsinkError::IoWithPath {
            path: entry.path.clone(),
            source: source_err,
        }
    })?;
    if opened != current {
        return Err(TsinkError::DataCorruption(format!(
            "file identity changed during inspection: {}",
            entry.path.display()
        )));
    }
    Ok(())
}

fn find_entry<'a>(entries: &'a [DiscoveredEntry], relative: &Path) -> Option<&'a DiscoveredEntry> {
    entries.iter().find(|entry| entry.relative == relative)
}

fn is_ignorable_empty_entry(entry: &DiscoveredEntry) -> bool {
    entry.relative == Path::new(".tsink.lock")
        || entry
            .relative
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".tsink-manifest.json.tmp-"))
}

fn is_link_like(metadata: &Metadata) -> bool {
    crate::engine::fs_utils::is_link_or_reparse_point(metadata)
}

fn bounded_message(mut message: String) -> String {
    const MAX_MESSAGE_BYTES: usize = 1024;
    if message.len() <= MAX_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_MESSAGE_BYTES.saturating_sub(3);
    while !message.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    message.truncate(end);
    message.push_str("...");
    message
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir as RawTempDir;

    struct TempDir(RawTempDir);

    impl TempDir {
        fn new() -> std::io::Result<Self> {
            let real_temp_root = std::fs::canonicalize(std::env::temp_dir())?;
            RawTempDir::new_in(real_temp_root).map(Self)
        }

        fn path(&self) -> &Path {
            self.0.path()
        }
    }

    fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
        fn visit(root: &Path, current: &Path, out: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
            let mut entries = std::fs::read_dir(current)
                .unwrap()
                .map(|entry| entry.unwrap())
                .collect::<Vec<_>>();
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let path = entry.path();
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                let meta = std::fs::symlink_metadata(&path).unwrap();
                if meta.file_type().is_dir() {
                    out.insert(relative, None);
                    visit(root, &path, out);
                } else if meta.file_type().is_file() {
                    out.insert(relative, Some(std::fs::read(path).unwrap()));
                } else {
                    out.insert(relative, None);
                }
            }
        }
        let mut out = BTreeMap::new();
        visit(root, root, &mut out);
        out
    }

    fn write_manifest(root: &Path, storage_version: u16, corrupt_crc: bool) {
        let payload = ManifestPayload {
            storage_format_version: storage_version,
            minimum_reader_storage_format_version: storage_version,
            creating_tsink_version: Some("fixture".to_string()),
            last_successfully_opened_tsink_version: None,
            format_affecting_features: CURRENT_FORMAT_FEATURES
                .into_iter()
                .map(str::to_string)
                .collect(),
            timestamp_precision: ManifestTimestampPrecision::Nanoseconds,
            chunk_point_capacity: 1024,
            partition_window_timestamp_units: 1,
        };
        let mut crc = crc32fast::hash(&serde_json::to_vec(&payload).unwrap());
        if corrupt_crc {
            crc ^= 1;
        }
        let envelope = ManifestEnvelope {
            magic: DATA_DIRECTORY_MAGIC.to_string(),
            manifest_schema_version: 1,
            payload_crc32: crc,
            payload,
        };
        std::fs::write(
            root.join(DATA_DIRECTORY_MANIFEST_FILE_NAME),
            serde_json::to_vec_pretty(&envelope).unwrap(),
        )
        .unwrap();
    }

    fn write_manifest_payload(root: &Path, payload: ManifestPayload) {
        let envelope = ManifestEnvelope {
            magic: DATA_DIRECTORY_MAGIC.to_string(),
            manifest_schema_version: 1,
            payload_crc32: crc32fast::hash(&serde_json::to_vec(&payload).unwrap()),
            payload,
        };
        std::fs::write(
            root.join(DATA_DIRECTORY_MANIFEST_FILE_NAME),
            serde_json::to_vec_pretty(&envelope).unwrap(),
        )
        .unwrap();
    }

    fn wal_frame(seq: u64, payload: &[u8]) -> Vec<u8> {
        wal_frame_with_type(seq, 1, payload)
    }

    fn wal_frame_with_type(seq: u64, frame_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; 24];
        out[0..4].copy_from_slice(&WAL_FRAME_MAGIC);
        out[4] = frame_type;
        out[8..16].copy_from_slice(&seq.to_le_bytes());
        out[16..20].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        out[20..24].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn valid_series_definition_payload(metric: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&(metric.len() as u16).to_le_bytes());
        payload.extend_from_slice(metric.as_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload
    }

    fn valid_samples_payload() -> Vec<u8> {
        let batch = crate::engine::wal::SamplesBatchFrame::from_points(
            1,
            crate::engine::chunk::ValueLane::Numeric,
            &[crate::engine::chunk::ChunkPoint {
                ts: 1,
                value: crate::Value::I64(1),
            }],
        )
        .unwrap();
        crate::engine::wal::FramedWal::encode_samples_frame_payload(&[batch]).unwrap()
    }

    fn write_published(root: &Path, segment: u64, frame: u64) {
        let bytes = encode_published_highwater_record(PublishedHighwaterRecord {
            highwater: WalHighWatermark { segment, frame },
            reset_through: None,
        });
        std::fs::write(root.join("wal/wal.published"), bytes).unwrap();
    }

    fn write_published_v2(
        root: &Path,
        segment: u64,
        frame: u64,
        reset_through_segment: u64,
        reset_through_frame: u64,
    ) {
        let bytes = encode_published_highwater_record(PublishedHighwaterRecord {
            highwater: WalHighWatermark { segment, frame },
            reset_through: Some(WalHighWatermark {
                segment: reset_through_segment,
                frame: reset_through_frame,
            }),
        });
        std::fs::write(root.join("wal/wal.published"), bytes).unwrap();
    }

    #[derive(Debug, Clone, Copy)]
    enum WalDamage {
        CorruptTail,
        MidLogChecksum,
    }

    fn create_current_directory(root: &Path) -> PathBuf {
        let storage = crate::StorageBuilder::new()
            .with_data_path(root)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        storage.close().unwrap();

        let wal_relative = std::fs::read_dir(root.join("wal"))
            .unwrap()
            .map(|entry| entry.unwrap())
            .find_map(|entry| {
                let relative = Path::new("wal").join(entry.file_name());
                wal_segment_id_from_relative(&relative).map(|_| relative)
            })
            .expect("current directory must contain one canonical WAL segment");
        wal_relative
    }

    fn remove_if_present(path: &Path) {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_dir() => std::fs::remove_dir_all(path).unwrap(),
            Ok(_) => std::fs::remove_file(path).unwrap(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => panic!("failed inspecting {}: {err}", path.display()),
        }
    }

    fn prune_salvage_v1_unsupported_state(root: &Path) {
        for relative in [
            "series_index.catalog.json",
            "segment_catalog.json",
            "series_index.catalog.d",
            "series_index.delta.bin",
            "series_index.delta.d",
            "lane_numeric/tombstones.json",
            "lane_numeric/tombstones.json.store",
            "lane_blob/tombstones.json",
            "lane_blob/tombstones.json.store",
        ] {
            remove_if_present(&root.join(relative));
        }
    }

    fn salvage_limits() -> DataDirectorySalvageLimits {
        DataDirectorySalvageLimits {
            source_is_offline_and_immutable: true,
            ..DataDirectorySalvageLimits::default()
        }
    }

    fn create_current_corrupt_wal_source(root: &Path, damage: WalDamage) -> (PathBuf, Vec<u8>) {
        let wal_relative = create_current_directory(root);
        prune_salvage_v1_unsupported_state(root);
        let first = wal_frame(1, &valid_series_definition_payload("retained_metric"));
        let mut bytes = first.clone();
        match damage {
            WalDamage::CorruptTail => {
                let mut second = wal_frame(2, &valid_series_definition_payload("discarded_metric"));
                second.pop();
                bytes.extend_from_slice(&second);
                write_published(root, 0, 2);
            }
            WalDamage::MidLogChecksum => {
                let mut second = wal_frame(2, &valid_series_definition_payload("damaged_metric"));
                *second.last_mut().unwrap() ^= 1;
                bytes.extend_from_slice(&second);
                bytes.extend_from_slice(&wal_frame(
                    3,
                    &valid_series_definition_payload("later_metric"),
                ));
                write_published(root, 0, 3);
            }
        }
        std::fs::write(root.join(&wal_relative), bytes).unwrap();
        (wal_relative, first)
    }

    fn salvage_staging_paths(parent: &Path) -> Vec<PathBuf> {
        let mut paths = std::fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".tmp-tsink-data-salvage-"))
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    #[test]
    fn empty_and_current_manifest_are_identified_without_mutation() {
        let temp = TempDir::new().unwrap();
        let before = tree_snapshot(temp.path());
        let empty =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(empty.format, DataDirectoryFormat::Empty);
        assert_eq!(before, tree_snapshot(temp.path()));

        write_manifest(temp.path(), crate::engine::STORAGE_FORMAT_VERSION, false);
        let before = tree_snapshot(temp.path());
        let current =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(current.format, DataDirectoryFormat::CurrentManifest);
        assert_eq!(current.health, InspectionHealth::FindingsPresent);
        assert!(current
            .findings
            .iter()
            .any(|finding| finding.code == "registry.snapshot_missing"));
        assert_eq!(before, tree_snapshot(temp.path()));
        assert!(!temp.path().join(".tsink.lock").exists());
    }

    #[test]
    fn normally_closed_current_directory_reports_unverified_catalog_and_is_read_only() {
        let temp = TempDir::new().unwrap();
        let storage = crate::StorageBuilder::new()
            .with_data_path(temp.path())
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        storage.close().unwrap();
        let before = tree_snapshot(temp.path());
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.format, DataDirectoryFormat::CurrentManifest);
        assert_eq!(report.health, InspectionHealth::FindingsPresent);
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "namespace.recovery_object_unverified"));
        assert!(report.completeness.complete);
        assert_eq!(before, tree_snapshot(temp.path()));
    }

    #[test]
    fn current_registry_snapshot_is_strictly_validated_and_memory_bounded() {
        let temp = TempDir::new().unwrap();
        create_current_directory(temp.path());
        let clean =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(clean.registry.valid);
        assert_eq!(clean.registry.series_count, Some(0));
        let required = clean.work.registry_modeled_bytes;
        assert!(required > 0);

        let exact = DataDirectoryInspectionLimits {
            max_registry_bytes: required,
            ..DataDirectoryInspectionLimits::default()
        };
        let exact_report = inspect_data_directory(temp.path(), exact).unwrap();
        assert!(exact_report.completeness.complete);
        let short = DataDirectoryInspectionLimits {
            max_registry_bytes: required - 1,
            ..exact
        };
        let short_report = inspect_data_directory(temp.path(), short).unwrap();
        assert!(!short_report.completeness.complete);
        assert!(short_report
            .completeness
            .bounds_hit
            .contains(&"max_registry_bytes".to_string()));

        std::fs::write(temp.path().join("series_index.bin"), b"corrupt registry").unwrap();
        let corrupt =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(corrupt.format, DataDirectoryFormat::CurrentManifest);
        assert_ne!(corrupt.health, InspectionHealth::Clean);
        assert!(corrupt
            .findings
            .iter()
            .any(|finding| finding.code == "registry.snapshot_corrupt"));

        std::fs::remove_file(temp.path().join("series_index.bin")).unwrap();
        let missing =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_ne!(missing.health, InspectionHealth::Clean);
        assert!(missing
            .findings
            .iter()
            .any(|finding| finding.code == "registry.snapshot_missing"));
    }

    #[test]
    fn newer_corrupt_and_foreign_inputs_remain_byte_for_byte_unchanged() {
        for case in ["newer", "corrupt", "foreign"] {
            let temp = TempDir::new().unwrap();
            match case {
                "newer" => write_manifest(
                    temp.path(),
                    crate::engine::STORAGE_FORMAT_VERSION.saturating_add(1),
                    false,
                ),
                "corrupt" => {
                    write_manifest(temp.path(), crate::engine::STORAGE_FORMAT_VERSION, true)
                }
                "foreign" => std::fs::write(temp.path().join("database.bin"), b"foreign").unwrap(),
                _ => unreachable!(),
            }
            let before = tree_snapshot(temp.path());
            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            let expected = match case {
                "newer" => DataDirectoryFormat::UnsupportedNewer,
                "corrupt" => DataDirectoryFormat::Corrupt,
                "foreign" => DataDirectoryFormat::UnknownForeign,
                _ => unreachable!(),
            };
            assert_eq!(report.format, expected);
            assert_eq!(before, tree_snapshot(temp.path()));
            assert!(!temp.path().join(".tsink.lock").exists());
        }
    }

    #[test]
    fn current_manifest_requires_exact_fields_features_and_nonzero_configuration() {
        let temp = TempDir::new().unwrap();
        write_manifest(temp.path(), crate::engine::STORAGE_FORMAT_VERSION, false);
        let path = temp.path().join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
        let mut unknown: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("future_field".to_string(), serde_json::json!(true));
        std::fs::write(&path, serde_json::to_vec_pretty(&unknown).unwrap()).unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.format, DataDirectoryFormat::Corrupt);
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "format.manifest_json_corrupt"));

        let invalid_payload = ManifestPayload {
            storage_format_version: crate::engine::STORAGE_FORMAT_VERSION,
            minimum_reader_storage_format_version: crate::engine::STORAGE_FORMAT_VERSION,
            creating_tsink_version: Some("fixture".to_string()),
            last_successfully_opened_tsink_version: None,
            format_affecting_features: vec![
                "framed_wal_v2".to_string(),
                "framed_wal_v2".to_string(),
            ],
            timestamp_precision: ManifestTimestampPrecision::Nanoseconds,
            chunk_point_capacity: 0,
            partition_window_timestamp_units: 0,
        };
        write_manifest_payload(temp.path(), invalid_payload);
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.format, DataDirectoryFormat::Corrupt);
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "format.manifest_configuration_invalid"));
    }

    #[test]
    fn namespace_and_wal_frame_bounds_are_exact() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        let frame = wal_frame(1, &valid_series_definition_payload("metric"));
        std::fs::write(temp.path().join("wal/wal.log"), &frame).unwrap();
        write_published(temp.path(), 0, 1);

        let entry_count = tree_snapshot(temp.path()).len() as u64;
        let exact = DataDirectoryInspectionLimits {
            max_namespace_entries: entry_count,
            max_wal_frames: 1,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert!(!report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_entries".to_string()));
        assert!(!report
            .completeness
            .bounds_hit
            .contains(&"max_wal_frames".to_string()));

        let mut short_namespace = exact;
        short_namespace.max_namespace_entries = entry_count - 1;
        let report = inspect_data_directory(temp.path(), short_namespace).unwrap();
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_entries".to_string()));

        let mut two_frames = frame;
        two_frames.extend_from_slice(&wal_frame(2, &valid_series_definition_payload("next")));
        std::fs::write(temp.path().join("wal/wal.log"), two_frames).unwrap();
        write_published(temp.path(), 0, 2);
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_wal_frames".to_string()));
    }

    #[test]
    fn byte_read_bound_is_exact_for_manifest() {
        let temp = TempDir::new().unwrap();
        write_manifest(temp.path(), crate::engine::STORAGE_FORMAT_VERSION, false);
        let len = std::fs::metadata(temp.path().join(DATA_DIRECTORY_MANIFEST_FILE_NAME))
            .unwrap()
            .len();
        let mut limits = DataDirectoryInspectionLimits {
            max_bytes_read: len,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), limits).unwrap();
        assert!(report.completeness.complete);
        limits.max_bytes_read = len - 1;
        let report = inspect_data_directory(temp.path(), limits).unwrap();
        assert!(!report.completeness.complete);
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_bytes_read".to_string()));
    }

    #[test]
    fn wal_truncation_and_checksum_damage_have_distinct_stable_codes() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        write_published(temp.path(), 0, 1);
        let mut tail = wal_frame(1, &valid_series_definition_payload("metric"));
        tail.pop();
        std::fs::write(temp.path().join("wal/wal.log"), tail).unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.corrupt_tail_payload"));
        assert_eq!(report.wal_segments[0].status, WalSegmentStatus::CorruptTail);

        let mut checksum = wal_frame(1, &valid_series_definition_payload("metric"));
        *checksum.last_mut().unwrap() ^= 1;
        std::fs::write(temp.path().join("wal/wal.log"), checksum).unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.mid_log_checksum_mismatch"));
        assert_eq!(
            report.wal_segments[0].status,
            WalSegmentStatus::MidLogCorruption
        );
    }

    #[test]
    fn wal_strict_payload_header_and_suffix_validation_rejects_crc_valid_garbage() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        write_published(temp.path(), 0, 1);
        std::fs::write(
            temp.path().join("wal/wal.log"),
            wal_frame(1, b"crc-valid but not a series definition"),
        )
        .unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.mid_log_payload_invalid"));

        let mut reserved = wal_frame(1, &valid_series_definition_payload("metric"));
        reserved[5] = 1;
        std::fs::write(temp.path().join("wal/wal.log"), reserved).unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.mid_log_reserved_header_invalid"));

        std::fs::write(
            temp.path().join("wal/wal.log"),
            wal_frame(0, &valid_series_definition_payload("metric")),
        )
        .unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.mid_log_sequence_invalid"));
    }

    #[test]
    fn first_mid_log_checksum_failure_discards_the_entire_remaining_suffix() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        let first = wal_frame(1, &valid_series_definition_payload("one"));
        let mut damaged = wal_frame(2, &valid_series_definition_payload("two"));
        *damaged.last_mut().unwrap() ^= 1;
        let third = wal_frame(3, &valid_series_definition_payload("three"));
        let damaged_start = first.len() as u64;
        let mut bytes = first;
        bytes.extend_from_slice(&damaged);
        bytes.extend_from_slice(&third);
        let file_len = bytes.len() as u64;
        std::fs::write(temp.path().join("wal/wal.log"), bytes).unwrap();
        write_published(temp.path(), 0, 3);
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.wal_segments[0].valid_frames, 1);
        assert!(report.discarded_ranges.iter().any(|range| {
            range.reason_code == "wal.mid_log_corruption"
                && range.bytes
                    == InspectionByteRange {
                        start: damaged_start,
                        end: file_len,
                    }
        }));
    }

    #[test]
    fn wal_decoded_memory_bound_is_exact() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        let payload = valid_series_definition_payload("metric");
        std::fs::write(temp.path().join("wal/wal.log"), wal_frame(1, &payload)).unwrap();
        write_published(temp.path(), 0, 1);
        let required =
            crate::engine::wal::modeled_frame_payload_for_inspection(1, &payload).unwrap() as u64;
        let exact = DataDirectoryInspectionLimits {
            max_wal_decoded_bytes: required,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert_eq!(report.wal_segments[0].status, WalSegmentStatus::Clean);
        let report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_wal_decoded_bytes: required - 1,
                ..exact
            },
        )
        .unwrap();
        assert_eq!(report.wal_segments[0].status, WalSegmentStatus::Incomplete);
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_wal_decoded_bytes".to_string()));
    }

    #[test]
    fn wal_series_definition_many_short_labels_is_admitted_at_exact_modeled_peak() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.push(b'm');
        payload.extend_from_slice(&128u16.to_le_bytes());
        for _ in 0..128 {
            payload.extend_from_slice(&1u16.to_le_bytes());
            payload.push(b'n');
            payload.extend_from_slice(&1u16.to_le_bytes());
            payload.push(b'v');
        }
        std::fs::write(temp.path().join("wal/wal.log"), wal_frame(1, &payload)).unwrap();
        write_published(temp.path(), 0, 1);
        let required =
            crate::engine::wal::modeled_frame_payload_for_inspection(1, &payload).unwrap() as u64;
        assert!(required > payload.len() as u64 * 2);
        let exact = DataDirectoryInspectionLimits {
            max_wal_decoded_bytes: required,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert_eq!(report.wal_segments[0].status, WalSegmentStatus::Clean);
        let report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_wal_decoded_bytes: required - 1,
                ..exact
            },
        )
        .unwrap();
        assert_eq!(report.wal_segments[0].status, WalSegmentStatus::Incomplete);
    }

    #[test]
    fn static_symlink_is_reported_and_never_followed() {
        #[cfg(unix)]
        {
            let temp = TempDir::new().unwrap();
            let outside = TempDir::new().unwrap();
            std::fs::write(outside.path().join("secret"), b"do not read").unwrap();
            std::os::unix::fs::symlink(outside.path(), temp.path().join("wal")).unwrap();
            let before = tree_snapshot(temp.path());
            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert!(report
                .findings
                .iter()
                .any(|finding| finding.code == "namespace.link_like_entry"));
            assert_eq!(report.work.bytes_read, 0);
            assert_eq!(before, tree_snapshot(temp.path()));
        }
    }

    #[cfg(unix)]
    #[test]
    fn inspection_refuses_a_symlinked_requested_path_ancestor() {
        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        std::fs::create_dir(outside.path().join("data")).unwrap();
        std::fs::write(outside.path().join("data/secret"), b"do not inspect").unwrap();
        std::os::unix::fs::symlink(outside.path(), temp.path().join("ancestor")).unwrap();

        let result = inspect_data_directory(
            temp.path().join("ancestor/data"),
            DataDirectoryInspectionLimits::default(),
        );
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn pinned_root_and_intermediate_handles_cannot_be_redirected_by_symlink_swaps() {
        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let root = temp.path().join("root");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/value"), b"inside").unwrap();
        std::fs::write(outside.path().join("value"), b"outside").unwrap();

        let root_handle = open_plain_directory_nofollow(&root, "test inspection root").unwrap();
        let source = InspectionSource {
            display: &root,
            root: &root_handle,
        };
        let mut ctx = InspectionContext::new(DataDirectoryInspectionLimits::default());
        let mut entries = Vec::new();
        walk_namespace(
            &mut ctx,
            &source,
            source.root,
            Path::new(""),
            0,
            &mut entries,
        )
        .unwrap();
        let value = find_entry(&entries, Path::new("nested/value")).unwrap();

        std::fs::rename(root.join("nested"), root.join("nested-original")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join("nested")).unwrap();
        assert!(open_regular_nofollow(&source, value).is_err());

        std::fs::rename(&root, temp.path().join("detached-root")).unwrap();
        std::os::unix::fs::symlink(outside.path(), &root).unwrap();
        let pinned = inspect_data_directory_from_open_root(
            &root,
            &root_handle,
            DataDirectoryInspectionLimits::default(),
        )
        .unwrap();
        assert!(pinned
            .findings
            .iter()
            .all(|finding| finding.path.as_deref() != Some("value")));
    }

    #[test]
    fn report_and_path_bounds_never_grow_past_limits() {
        let temp = TempDir::new().unwrap();
        for index in 0..10 {
            std::fs::write(temp.path().join(format!("foreign-path-{index:02}")), b"x").unwrap();
        }
        let limits = DataDirectoryInspectionLimits {
            max_report_items: 3,
            max_issues: 2,
            max_retained_path_bytes: 64,
            max_path_bytes: 32,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), limits).unwrap();
        assert!(report.work.report_items <= 3);
        assert!(report.work.issues <= 2);
        assert!(report.work.retained_path_bytes <= 64);
        assert_eq!(
            report.work.report_items,
            u64::try_from(
                report.findings.len()
                    + report.discarded_ranges.len()
                    + report.wal_segments.len()
                    + report.segments.len()
            )
            .unwrap()
        );
        assert!(!report.completeness.complete);
    }

    #[test]
    fn persisted_salvage_report_preflight_bounds_arrays_before_typed_decode() {
        let bytes =
            br#"{"discarded_ranges":[{},{}],"omitted_wal_paths":[],"path_dispositions":[]}"#;
        let exact = DataDirectoryInspectionLimits {
            max_report_items: 2,
            ..DataDirectoryInspectionLimits::default()
        };
        assert_eq!(
            preflight_persisted_salvage_report_json(bytes, exact),
            Ok(())
        );
        assert_eq!(
            preflight_persisted_salvage_report_json(
                bytes,
                DataDirectoryInspectionLimits {
                    max_report_items: 1,
                    ..exact
                }
            ),
            Err(PersistedReportPreflightError::ReportItems)
        );
        assert_eq!(
            preflight_persisted_salvage_report_json(
                br#"{"source_path":"escaped\u002fpath"}"#,
                exact
            ),
            Err(PersistedReportPreflightError::SyntaxEnvelope)
        );
    }

    #[test]
    fn malformed_wal_magic_is_mid_log_and_source_is_unchanged() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        let mut frame = wal_frame(1, &valid_series_definition_payload("metric"));
        frame[0] ^= 1;
        std::fs::write(temp.path().join("wal/wal.log"), frame).unwrap();
        write_published(temp.path(), 0, 1);
        let before = tree_snapshot(temp.path());
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.mid_log_magic_mismatch"));
        assert_eq!(before, tree_snapshot(temp.path()));
    }

    #[test]
    fn file_hash_bound_is_exact() {
        let temp = TempDir::new().unwrap();
        let lane = temp
            .path()
            .join("lane_numeric/segments/L0/seg-0000000000000001");
        std::fs::create_dir_all(&lane).unwrap();
        let mut manifest = vec![0u8; SEGMENT_MANIFEST_BYTES];
        manifest[0..4].copy_from_slice(&SEGMENT_MANIFEST_MAGIC);
        manifest[4..6].copy_from_slice(&2u16.to_le_bytes());
        manifest[8..16].copy_from_slice(&1u64.to_le_bytes());
        manifest[16] = 0;
        manifest[88..92].copy_from_slice(&4u32.to_le_bytes());
        for (index, (kind, name)) in [
            (1u8, "chunks.bin"),
            (2, "chunk_index.bin"),
            (3, "series.bin"),
            (4, "postings.bin"),
        ]
        .into_iter()
        .enumerate()
        {
            let bytes = [kind; 3];
            std::fs::write(lane.join(name), bytes).unwrap();
            let offset = 96 + index * 20;
            manifest[offset] = kind;
            manifest[offset + 4..offset + 12].copy_from_slice(&3u64.to_le_bytes());
            manifest[offset + 12..offset + 20]
                .copy_from_slice(&xxhash_rust::xxh64::xxh64(&bytes, 0).to_le_bytes());
        }
        let crc = crc32fast::hash(&manifest[..SEGMENT_MANIFEST_BYTES - 4]);
        manifest[SEGMENT_MANIFEST_BYTES - 4..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(lane.join("manifest.bin"), manifest).unwrap();

        let exact = DataDirectoryInspectionLimits {
            max_bytes_hashed: (SEGMENT_MANIFEST_BYTES - 4 + 12) as u64,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert_eq!(report.segments[0].status, PersistedSegmentStatus::Clean);
        let mut short = exact;
        short.max_bytes_hashed = exact.max_bytes_hashed - 1;
        let report = inspect_data_directory(temp.path(), short).unwrap();
        assert_eq!(
            report.segments[0].status,
            PersistedSegmentStatus::Incomplete
        );
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_bytes_hashed".to_string()));
    }

    #[test]
    fn unexpected_segment_file_and_catalog_missing_orphan_are_reported() {
        let temp = TempDir::new().unwrap();
        let root = temp
            .path()
            .join("lane_numeric/segments/L0/seg-0000000000000001");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("surprise"), b"x").unwrap();
        std::fs::write(
            temp.path().join("series_index.catalog.json"),
            br#"{"version":2,"segments":[{"lane":"numeric","level":0,"segment_id":2}]}"#,
        )
        .unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "segment.unexpected_file"));
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "catalog.referenced_segment_missing"));
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "catalog.unreferenced_segment"));
    }

    #[test]
    fn catalog_reference_arrays_are_bounded_before_typed_decode() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("series_index.catalog.json"),
            br#"{"version":2,"segments":[{"lane":"numeric","level":0,"segment_id":1},{"lane":"numeric","level":0,"segment_id":2}]}"#,
        )
        .unwrap();
        let exact = DataDirectoryInspectionLimits {
            max_segments: 2,
            ..DataDirectoryInspectionLimits::default()
        };
        let exact_report = inspect_data_directory(temp.path(), exact).unwrap();
        assert!(!exact_report
            .completeness
            .bounds_hit
            .contains(&"max_segments".to_string()));

        let short_report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_segments: 1,
                ..exact
            },
        )
        .unwrap();
        assert!(short_report
            .completeness
            .bounds_hit
            .contains(&"max_segments".to_string()));
        assert!(short_report
            .findings
            .iter()
            .any(|finding| finding.code == "catalog.decode_limit_exceeded"));
    }

    #[test]
    fn foreign_lane_descendants_and_uppercase_catalog_names_are_not_whitelisted() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("lane_numeric/foreign")).unwrap();
        std::fs::write(temp.path().join("lane_numeric/foreign/payload"), b"x").unwrap();
        std::fs::create_dir(temp.path().join("series_index.catalog.d")).unwrap();
        std::fs::write(
            temp.path()
                .join("series_index.catalog.d/segment-numeric-0A-000000000000000A.json"),
            b"{}",
        )
        .unwrap();
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        let unknown_paths = report
            .findings
            .iter()
            .filter(|finding| finding.code == "namespace.unknown_path")
            .count();
        assert!(unknown_paths >= 3, "{:#?}", report.findings);
    }

    #[test]
    fn recognized_namespace_containers_require_exact_filesystem_kinds() {
        for (case, expected_path) in [
            ("lock", ".tsink.lock"),
            ("wal", "wal"),
            ("numeric_lane", "lane_numeric"),
            ("blob_lane", "lane_blob"),
            ("segments", "lane_numeric/segments"),
            ("level", "lane_numeric/segments/L0"),
        ] {
            let temp = TempDir::new().unwrap();
            create_current_directory(temp.path());
            match case {
                "lock" => {
                    let path = temp.path().join(".tsink.lock");
                    match std::fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => panic!("remove lock fixture: {err}"),
                    }
                    std::fs::create_dir(path).unwrap();
                }
                "wal" => {
                    std::fs::remove_dir_all(temp.path().join("wal")).unwrap();
                    std::fs::write(temp.path().join("wal"), b"not a directory").unwrap();
                }
                "numeric_lane" => {
                    std::fs::write(temp.path().join("lane_numeric"), b"not a directory").unwrap();
                }
                "blob_lane" => {
                    std::fs::write(temp.path().join("lane_blob"), b"not a directory").unwrap();
                }
                "segments" => {
                    std::fs::create_dir(temp.path().join("lane_numeric")).unwrap();
                    std::fs::write(
                        temp.path().join("lane_numeric/segments"),
                        b"not a directory",
                    )
                    .unwrap();
                }
                "level" => {
                    std::fs::create_dir_all(temp.path().join("lane_numeric/segments")).unwrap();
                    std::fs::write(
                        temp.path().join("lane_numeric/segments/L0"),
                        b"not a directory",
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }

            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert_ne!(report.health, InspectionHealth::Clean, "{case}");
            assert!(report.findings.iter().any(|finding| {
                finding.code == "namespace.known_path_kind_invalid"
                    && finding.path.as_deref() == Some(expected_path)
            }));

            if case == "wal" {
                assert!(crate::StorageBuilder::new()
                    .with_data_path(temp.path())
                    .with_background_threads_enabled_for_tests(false)
                    .build()
                    .is_err());
            }
        }
    }

    #[test]
    fn namespace_depth_bound_is_exact() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("cluster/member")).unwrap();
        std::fs::write(temp.path().join("cluster/member/state"), b"x").unwrap();
        let exact = DataDirectoryInspectionLimits {
            max_namespace_depth: 2,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert!(!report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_depth".to_string()));
        let report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_namespace_depth: 1,
                ..exact
            },
        )
        .unwrap();
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_depth".to_string()));
        assert!(inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_namespace_depth: MAX_INSPECTION_NAMESPACE_DEPTH + 1,
                ..exact
            },
        )
        .is_err());
    }

    #[test]
    fn nested_namespace_siblings_never_overrun_the_global_entry_bound() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("a-directory")).unwrap();
        std::fs::write(temp.path().join("a-directory/child"), b"x").unwrap();
        std::fs::write(temp.path().join("z-sibling"), b"x").unwrap();

        let exact = DataDirectoryInspectionLimits {
            max_namespace_entries: 3,
            ..DataDirectoryInspectionLimits::default()
        };
        let exact_report = inspect_data_directory(temp.path(), exact).unwrap();
        assert_eq!(exact_report.work.namespace_entries, 3);
        assert!(!exact_report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_entries".to_string()));

        let short_report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_namespace_entries: 2,
                ..exact
            },
        )
        .unwrap();
        assert_eq!(short_report.work.namespace_entries, 2);
        assert!(short_report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_entries".to_string()));
    }

    #[test]
    fn namespace_retained_memory_bound_is_exact() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("nested")).unwrap();
        std::fs::write(
            temp.path().join("nested/long-but-bounded-namespace-entry"),
            b"x",
        )
        .unwrap();
        std::fs::write(temp.path().join("sibling"), b"x").unwrap();

        let measured =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        let required = measured.work.namespace_retained_bytes;
        assert!(required > 0);
        let exact = DataDirectoryInspectionLimits {
            max_namespace_retained_bytes: required,
            ..DataDirectoryInspectionLimits::default()
        };
        let exact_report = inspect_data_directory(temp.path(), exact).unwrap();
        assert_eq!(exact_report.work.namespace_retained_bytes, required);
        assert!(!exact_report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_retained_bytes".to_string()));

        let short_report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_namespace_retained_bytes: required - 1,
                ..exact
            },
        )
        .unwrap();
        assert!(short_report.work.namespace_retained_bytes < required);
        assert!(short_report
            .completeness
            .bounds_hit
            .contains(&"max_namespace_retained_bytes".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn opaque_namespace_component_is_never_collapsed_into_a_known_parent() {
        use std::os::unix::ffi::OsStringExt;

        let opaque = std::ffi::OsString::from_vec(vec![0xff, b'x']);
        let relative = Path::new("lane_numeric").join(opaque);
        assert!(utf8_path_components(&relative).is_none());
        assert_eq!(expected_known_path_kind(&relative), None);
        assert!(!is_known_namespace_path(&relative, EntryKind::File));

        let entry = DiscoveredEntry {
            path: Path::new("/pinned-root").join(&relative),
            relative,
            kind: EntryKind::File,
            len: 1,
            identity: FileIdentity {
                device: 1,
                inode: 1,
            },
        };
        let mut ctx = InspectionContext::new(DataDirectoryInspectionLimits::default());
        report_unknown_namespace(&mut ctx, &[entry]);
        assert!(ctx
            .findings
            .iter()
            .any(|finding| finding.code == "namespace.opaque_path_component"));
        assert!(ctx
            .findings
            .iter()
            .any(|finding| finding.code == "namespace.unknown_path"));
        assert!(ctx
            .findings
            .iter()
            .filter_map(|finding| finding.path.as_deref())
            .all(|path| path == "lane_numeric/%FFx"));
    }

    #[cfg(unix)]
    #[test]
    fn rendered_paths_percent_encode_opaque_and_reserved_bytes_without_collisions() {
        use std::os::unix::ffi::OsStringExt;

        let mut ctx = InspectionContext::new(DataDirectoryInspectionLimits::default());
        let opaque = PathBuf::from(std::ffi::OsString::from_vec(vec![
            0xff, b'%', b'?', b'"', b'\\',
        ]));
        let rendered = ctx.rendered_path(&opaque).unwrap();
        assert_eq!(rendered, "%FF%25?%22%5C");

        let literal = Path::new("%FF%25?%22%5C");
        let literal_rendered = ctx.rendered_path(literal).unwrap();
        assert_eq!(literal_rendered, "%25FF%2525?%2522%255C");
        assert_ne!(rendered, literal_rendered);
    }

    #[test]
    fn truncated_segment_before_later_wal_is_globally_mid_log() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        let first = wal_frame(1, &valid_series_definition_payload("first"));
        let mut truncated = wal_frame(2, &valid_series_definition_payload("truncated"));
        truncated.pop();
        let mut bytes = first;
        bytes.extend_from_slice(&truncated);
        std::fs::write(temp.path().join("wal/wal.log"), bytes).unwrap();
        std::fs::write(
            temp.path().join("wal/wal-0000000000000001.log"),
            wal_frame(2, &valid_series_definition_payload("later")),
        )
        .unwrap();
        write_published(temp.path(), 1, 3);

        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(
            report.wal_segments[0].status,
            WalSegmentStatus::MidLogCorruption
        );
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.mid_log_truncation_before_later_segment"));
        assert!(report.discarded_ranges.iter().any(|range| {
            range.path == "wal/wal-0000000000000001.log"
                && range.bytes.start == 0
                && range.first_frame == Some(2)
                && range.reason_code == "wal.after_first_unsafe_range"
        }));
    }

    #[test]
    fn salvage_corrupt_tail_preserves_source_and_strictly_reopens_clean_destination() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        let (wal_relative, _retained) =
            create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        let source_before = tree_snapshot(&source);

        let report = salvage_data_directory(&source, &destination, salvage_limits()).unwrap();

        assert_eq!(source_before, tree_snapshot(&source));
        assert_eq!(
            report.retained_wal_highwater,
            SalvagedWalHighwater {
                segment: 0,
                frame: 0,
            }
        );
        assert!(std::fs::read(destination.join(&wal_relative))
            .unwrap()
            .is_empty());
        assert_eq!(report.recovered_inspection.health, InspectionHealth::Clean);
        assert!(report.recovered_inspection.discarded_ranges.is_empty());
        assert_eq!(
            report
                .recovered_inspection
                .wal_published
                .reset_through_segment,
            Some(0)
        );
        assert_eq!(
            report
                .recovered_inspection
                .wal_published
                .reset_through_frame,
            Some(0)
        );
        let marker_bytes = std::fs::read(destination.join("wal/wal.published")).unwrap();
        assert_eq!(marker_bytes.len(), WAL_PUBLISHED_V2_BYTES as usize);
        let marker = decode_published_highwater_record(&marker_bytes).unwrap();
        assert_eq!(
            marker.highwater,
            WalHighWatermark {
                segment: 0,
                frame: 0,
            }
        );
        assert_eq!(marker.reset_through, Some(marker.highwater));
        let destination_file_bytes = tree_snapshot(&destination)
            .into_values()
            .flatten()
            .map(|bytes| bytes.len() as u64)
            .sum::<u64>();
        assert_eq!(report.copied_bytes, destination_file_bytes);
        assert!(report.discarded_ranges.iter().any(|range| {
            range.reason_code == "wal.salvage_v1_full_reset"
                && range.path == wal_relative.to_string_lossy()
                && range.bytes.start == 0
                && range.bytes.end == std::fs::metadata(source.join(&wal_relative)).unwrap().len()
        }));
        let persisted: PersistedSalvageReport = serde_json::from_slice(
            &std::fs::read(destination.join(&report.persisted_report_path)).unwrap(),
        )
        .unwrap();
        assert_eq!(persisted.discarded_ranges, report.discarded_ranges);
        assert_eq!(
            persisted.retained_wal_highwater,
            report.retained_wal_highwater
        );
        assert!(!destination.join(".tsink.lock").exists());

        let reopened = crate::StorageBuilder::new()
            .with_data_path(&destination)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        reopened
            .insert_rows(&[crate::Row::new(
                "after_salvage",
                crate::DataPoint::new(1, 1.0),
            )])
            .unwrap();
        reopened.close().unwrap();
        let strict_reopen = crate::StorageBuilder::new()
            .with_data_path(&destination)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        strict_reopen.close().unwrap();
    }

    #[test]
    fn wal_sequences_are_globally_consecutive_across_segment_files() {
        for (case, second_sequence) in [("duplicate", 10), ("decrease", 9), ("gap", 12)] {
            let temp = TempDir::new().unwrap();
            std::fs::create_dir(temp.path().join("wal")).unwrap();
            std::fs::write(
                temp.path().join("wal/wal.log"),
                wal_frame(10, &valid_series_definition_payload("first")),
            )
            .unwrap();
            std::fs::write(
                temp.path().join("wal/wal-0000000000000001.log"),
                wal_frame(second_sequence, &valid_series_definition_payload("second")),
            )
            .unwrap();
            write_published(temp.path(), 1, second_sequence);

            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert_eq!(
                report.wal_segments[1].status,
                WalSegmentStatus::MidLogCorruption,
                "{case}: {:#?}",
                report.findings
            );
            assert_eq!(report.wal_segments[1].valid_frames, 0);
            assert!(report.findings.iter().any(|finding| {
                finding.code == "wal.mid_log_sequence_invalid"
                    && finding.path.as_deref() == Some("wal/wal-0000000000000001.log")
            }));
        }
    }

    #[test]
    fn wal_published_inspection_decodes_legacy_and_v2_reset_floor() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(temp.path().join("wal/wal.log"), []).unwrap();

        write_published(temp.path(), 0, 0);
        let legacy =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(legacy.wal_published.checksum_valid, Some(true));
        assert_eq!(legacy.wal_published.segment, Some(0));
        assert_eq!(legacy.wal_published.frame, Some(0));
        assert_eq!(legacy.wal_published.reset_through_segment, None);
        assert_eq!(legacy.wal_published.reset_through_frame, None);
        assert_eq!(legacy.work.bytes_hashed, 20);

        write_published_v2(temp.path(), 0, 0, 0, 0);
        let v2 =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(v2.wal_published.checksum_valid, Some(true));
        assert_eq!(v2.wal_published.segment, Some(0));
        assert_eq!(v2.wal_published.frame, Some(0));
        assert_eq!(v2.wal_published.reset_through_segment, Some(0));
        assert_eq!(v2.wal_published.reset_through_frame, Some(0));
        assert_eq!(v2.work.bytes_hashed, 36);

        let legacy_json: InspectedWalPublishedMarker = serde_json::from_value(serde_json::json!({
            "present": true,
            "checksum_valid": true,
            "segment": 0,
            "frame": 0,
            "frame_type": null
        }))
        .unwrap();
        assert_eq!(legacy_json.reset_through_segment, None);
        assert_eq!(legacy_json.reset_through_frame, None);
    }

    #[test]
    fn wal_published_v2_hash_bound_is_exact() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(temp.path().join("wal/wal.log"), []).unwrap();
        write_published_v2(temp.path(), 0, 0, 0, 0);

        let exact = DataDirectoryInspectionLimits {
            max_bytes_hashed: 36,
            ..DataDirectoryInspectionLimits::default()
        };
        let report = inspect_data_directory(temp.path(), exact).unwrap();
        assert_eq!(report.wal_published.checksum_valid, Some(true));
        assert_eq!(report.work.bytes_hashed, 36);

        let report = inspect_data_directory(
            temp.path(),
            DataDirectoryInspectionLimits {
                max_bytes_hashed: 35,
                ..exact
            },
        )
        .unwrap();
        assert_eq!(report.wal_published.checksum_valid, None);
        assert!(report
            .completeness
            .bounds_hit
            .contains(&"max_bytes_hashed".to_string()));
    }

    #[test]
    fn wal_published_inspection_rejects_noncanonical_record_sizes_without_reading() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(temp.path().join("wal/wal.log"), []).unwrap();
        for len in [23usize, 25, 39, 41] {
            std::fs::write(temp.path().join("wal/wal.published"), vec![0; len]).unwrap();
            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert_eq!(report.wal_published.checksum_valid, Some(false), "{len}");
            assert_eq!(report.work.bytes_read, 0, "{len}");
            assert!(report
                .findings
                .iter()
                .any(|finding| finding.code == "wal.published_invalid_size_or_type"));
        }
    }

    #[test]
    fn wal_published_inspection_rejects_corrupt_or_invalid_v2_records() {
        for case in ["checksum", "reset_above_published"] {
            let temp = TempDir::new().unwrap();
            std::fs::create_dir(temp.path().join("wal")).unwrap();
            std::fs::write(temp.path().join("wal/wal.log"), []).unwrap();
            let mut bytes = encode_published_highwater_record(PublishedHighwaterRecord {
                highwater: WalHighWatermark {
                    segment: 0,
                    frame: 1,
                },
                reset_through: Some(WalHighWatermark {
                    segment: 0,
                    frame: if case == "checksum" { 1 } else { 2 },
                }),
            });
            if case == "checksum" {
                *bytes.last_mut().unwrap() ^= 1;
            }
            std::fs::write(temp.path().join("wal/wal.published"), bytes).unwrap();

            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert_eq!(report.wal_published.checksum_valid, Some(false), "{case}");
            assert_eq!(report.wal_published.segment, None, "{case}");
            assert_eq!(report.wal_published.frame, None, "{case}");
            assert_eq!(report.wal_published.reset_through_segment, None, "{case}");
            assert_eq!(report.wal_published.reset_through_frame, None, "{case}");
            assert!(report
                .findings
                .iter()
                .any(|finding| finding.code == "wal.published_corrupt"));
        }
    }

    #[test]
    fn reset_floor_accepts_an_empty_boundary_or_a_clean_file_beginning_after_it() {
        for case in ["empty", "begins_after"] {
            let temp = TempDir::new().unwrap();
            std::fs::create_dir(temp.path().join("wal")).unwrap();
            let bytes = match case {
                "empty" => Vec::new(),
                "begins_after" => wal_frame(3, &valid_series_definition_payload("after_reset")),
                _ => unreachable!(),
            };
            std::fs::write(temp.path().join("wal/wal.log"), bytes).unwrap();
            write_published_v2(temp.path(), 0, 2, 0, 2);

            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert_eq!(report.wal_published.reset_through_segment, Some(0));
            assert_eq!(report.wal_published.reset_through_frame, Some(2));
            assert_eq!(report.wal_segments[0].status, WalSegmentStatus::Clean);
            assert!(
                !report
                    .findings
                    .iter()
                    .any(|finding| finding.code == "wal.published_boundary_missing"),
                "{case}: {report:#?}"
            );
        }
    }

    #[test]
    fn post_reset_commit_still_requires_its_exact_samples_boundary() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(
            temp.path().join("wal/wal.log"),
            wal_frame_with_type(3, 2, &valid_samples_payload()),
        )
        .unwrap();
        write_published_v2(temp.path(), 0, 3, 0, 2);

        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.wal_published.frame_type, Some(2));
        assert_eq!(report.wal_published.reset_through_segment, Some(0));
        assert_eq!(report.wal_published.reset_through_frame, Some(2));
        assert!(
            !report.findings.iter().any(|finding| matches!(
                finding.code.as_str(),
                "wal.published_boundary_missing" | "wal.published_not_logical_boundary"
            )),
            "{report:#?}"
        );
    }

    #[test]
    fn legacy_marker_requires_contiguous_replay_interval_segment_ids() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(
            temp.path().join("wal/wal.log"),
            wal_frame(1, &valid_series_definition_payload("floor")),
        )
        .unwrap();
        std::fs::write(
            temp.path().join("wal/wal-0000000000000002.log"),
            wal_frame_with_type(2, 2, &valid_samples_payload()),
        )
        .unwrap();
        write_published(temp.path(), 2, 2);

        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.wal_published.frame_type, Some(2));
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "wal.published_boundary_missing"),
            "{report:#?}"
        );
    }

    #[test]
    fn v2_reset_floor_requires_contiguous_replay_interval_segment_ids() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(
            temp.path().join("wal/wal-0000000000000001.log"),
            wal_frame(2, &valid_series_definition_payload("after_reset")),
        )
        .unwrap();
        std::fs::write(
            temp.path().join("wal/wal-0000000000000003.log"),
            wal_frame_with_type(3, 2, &valid_samples_payload()),
        )
        .unwrap();
        write_published_v2(temp.path(), 3, 3, 1, 1);

        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert_eq!(report.wal_published.frame_type, Some(2));
        assert_eq!(report.wal_published.reset_through_segment, Some(1));
        assert_eq!(report.wal_published.reset_through_frame, Some(1));
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "wal.published_boundary_missing"),
            "{report:#?}"
        );
    }

    #[test]
    fn reset_floor_rejects_missing_short_corrupt_or_uncovered_boundaries() {
        for case in ["missing", "short", "corrupt", "zero_corrupt", "uncovered"] {
            let temp = TempDir::new().unwrap();
            std::fs::create_dir(temp.path().join("wal")).unwrap();
            match case {
                "missing" => {
                    std::fs::write(temp.path().join("wal/wal.log"), []).unwrap();
                    write_published_v2(temp.path(), 1, 2, 1, 2);
                }
                "short" => {
                    std::fs::write(
                        temp.path().join("wal/wal.log"),
                        wal_frame(1, &valid_series_definition_payload("short")),
                    )
                    .unwrap();
                    write_published_v2(temp.path(), 0, 2, 0, 2);
                }
                "corrupt" => {
                    std::fs::write(temp.path().join("wal/wal.log"), b"bad").unwrap();
                    write_published_v2(temp.path(), 0, 2, 0, 2);
                }
                "zero_corrupt" => {
                    std::fs::write(temp.path().join("wal/wal.log"), b"bad").unwrap();
                    write_published_v2(temp.path(), 0, 0, 0, 0);
                }
                "uncovered" => {
                    std::fs::write(
                        temp.path().join("wal/wal.log"),
                        wal_frame(3, &valid_series_definition_payload("after_floor")),
                    )
                    .unwrap();
                    write_published_v2(temp.path(), 0, 2, 0, 1);
                }
                _ => unreachable!(),
            }

            let report =
                inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert!(
                report
                    .findings
                    .iter()
                    .any(|finding| finding.code == "wal.published_boundary_missing"),
                "{case}: {report:#?}"
            );
        }
    }

    #[test]
    fn wal_publication_marker_must_end_a_logical_samples_boundary() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir(temp.path().join("wal")).unwrap();
        std::fs::write(
            temp.path().join("wal/wal.log"),
            wal_frame(1, &valid_series_definition_payload("orphan-definition")),
        )
        .unwrap();
        write_published(temp.path(), 0, 1);
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "wal.published_not_logical_boundary"));
        assert!(report
            .findings
            .iter()
            .any(|finding| { finding.code == "wal.production_replay_semantics_unverified" }));

        std::fs::write(temp.path().join("wal/wal-0000000000000001.log"), b"").unwrap();
        write_published(temp.path(), 1, 0);
        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| { finding.code == "wal.published_predecessor_not_logical_boundary" }));
    }

    #[test]
    fn clean_persisted_segment_covers_a_published_marker_after_wal_reset() {
        let temp = TempDir::new().unwrap();
        let storage = crate::StorageBuilder::new()
            .with_data_path(temp.path())
            .with_timestamp_precision(crate::TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                crate::Row::new("persisted_boundary", crate::DataPoint::new(1, 1.0)),
                crate::Row::new("persisted_boundary", crate::DataPoint::new(2, 2.0)),
                crate::Row::new("persisted_boundary", crate::DataPoint::new(3, 3.0)),
            ])
            .unwrap();
        storage.close().unwrap();

        let report =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(
            report
                .segments
                .iter()
                .any(|segment| segment.status == PersistedSegmentStatus::Clean),
            "{report:#?}"
        );
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.code == "wal.published_boundary_missing"),
            "{report:#?}"
        );
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.severity == InspectionSeverity::Error),
            "{report:#?}"
        );

        write_published(temp.path(), u64::MAX, u64::MAX);
        let impossible =
            inspect_data_directory(temp.path(), DataDirectoryInspectionLimits::default()).unwrap();
        assert!(
            impossible
                .findings
                .iter()
                .any(|finding| finding.code == "wal.published_boundary_missing"),
            "{impossible:#?}"
        );
    }

    #[test]
    fn salvage_missing_published_frame_gap_resets_all_wal_and_reopens() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        let wal_relative = create_current_directory(&source);
        prune_salvage_v1_unsupported_state(&source);
        let first = wal_frame(4, &valid_series_definition_payload("retained_metric"));
        let mut bytes = first;
        bytes.extend_from_slice(&wal_frame(
            6,
            &valid_series_definition_payload("after_missing_frame"),
        ));
        std::fs::write(source.join(&wal_relative), bytes).unwrap();
        write_published(&source, 0, 5);
        let source_before = tree_snapshot(&source);

        let report = salvage_data_directory(&source, &destination, salvage_limits()).unwrap();

        assert_eq!(source_before, tree_snapshot(&source));
        assert_eq!(
            report.retained_wal_highwater,
            SalvagedWalHighwater {
                segment: 0,
                frame: 0,
            }
        );
        assert!(std::fs::read(destination.join(wal_relative))
            .unwrap()
            .is_empty());
        let reopened = crate::StorageBuilder::new()
            .with_data_path(&destination)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn salvage_mid_log_checksum_damage_discards_the_entire_suffix() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        let (wal_relative, _retained) =
            create_current_corrupt_wal_source(&source, WalDamage::MidLogChecksum);
        let source_before = tree_snapshot(&source);

        let report = salvage_data_directory(&source, &destination, salvage_limits()).unwrap();

        assert_eq!(source_before, tree_snapshot(&source));
        assert!(std::fs::read(destination.join(wal_relative))
            .unwrap()
            .is_empty());
        assert_eq!(
            report.retained_wal_highwater,
            SalvagedWalHighwater {
                segment: 0,
                frame: 0,
            }
        );
        assert!(report
            .discarded_ranges
            .iter()
            .any(|range| range.reason_code == "wal.mid_log_corruption"));
    }

    #[test]
    fn salvage_omits_later_wal_segments_after_the_first_unsafe_range() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        let (first_wal, _retained) =
            create_current_corrupt_wal_source(&source, WalDamage::MidLogChecksum);
        let later_relative = PathBuf::from("wal/wal-0000000000000001.log");
        std::fs::write(
            source.join(&later_relative),
            wal_frame(2, &valid_series_definition_payload("later_valid_segment")),
        )
        .unwrap();
        write_published(&source, 1, 3);

        let report = salvage_data_directory(&source, &destination, salvage_limits()).unwrap();

        assert!(std::fs::read(destination.join(first_wal))
            .unwrap()
            .is_empty());
        assert!(!destination.join(&later_relative).exists());
        assert_eq!(
            report.omitted_wal_paths,
            vec!["wal/wal-0000000000000001.log".to_string()]
        );
        assert!(report.discarded_ranges.iter().any(|range| {
            range.path == "wal/wal-0000000000000001.log"
                && range.reason_code == "wal.salvage_v1_full_reset"
        }));
    }

    #[test]
    fn salvage_report_accepts_corruption_that_starts_after_the_retained_wal() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        let first_relative = create_current_directory(&source);
        prune_salvage_v1_unsupported_state(&source);
        std::fs::write(
            source.join(&first_relative),
            wal_frame(1, &valid_series_definition_payload("healthy_first")),
        )
        .unwrap();
        let later_relative = PathBuf::from("wal/wal-0000000000000001.log");
        let mut later = wal_frame(2, &valid_series_definition_payload("healthy_later_prefix"));
        let mut corrupt_tail = wal_frame(3, &valid_series_definition_payload("corrupt_later_tail"));
        corrupt_tail.pop();
        later.extend_from_slice(&corrupt_tail);
        std::fs::write(source.join(&later_relative), later).unwrap();
        write_published(&source, 1, 3);

        let report = salvage_data_directory(&source, &destination, salvage_limits()).unwrap();

        assert!(std::fs::read(destination.join(first_relative))
            .unwrap()
            .is_empty());
        assert!(!destination.join(&later_relative).exists());
        assert_eq!(
            report.omitted_wal_paths,
            vec!["wal/wal-0000000000000001.log".to_string()]
        );
        assert!(report.discarded_ranges.iter().any(|range| {
            range.path == "wal/wal-0000000000000001.log" && range.reason_code == "wal.corrupt_tail"
        }));
        let inspected =
            inspect_data_directory(&destination, DataDirectoryInspectionLimits::default()).unwrap();
        assert!(
            !inspected
                .findings
                .iter()
                .any(|finding| finding.code == "salvage.report_invariant_invalid"),
            "{inspected:?}"
        );
    }

    #[test]
    fn salvage_report_round_trips_an_omitted_empty_later_wal_segment() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::MidLogChecksum);
        let later_relative = PathBuf::from("wal/wal-0000000000000001.log");
        std::fs::write(source.join(&later_relative), []).unwrap();

        let report = salvage_data_directory(&source, &destination, salvage_limits()).unwrap();
        assert_eq!(
            report.omitted_wal_paths,
            vec!["wal/wal-0000000000000001.log".to_string()]
        );
        assert!(report.discarded_ranges.iter().any(|range| {
            range.path == "wal/wal-0000000000000001.log"
                && range.reason_code == "wal.salvage_v1_full_reset"
                && range.bytes.start == 0
                && range.bytes.end == 0
        }));

        let inspected =
            inspect_data_directory(&destination, DataDirectoryInspectionLimits::default()).unwrap();
        assert!(
            !inspected
                .findings
                .iter()
                .any(|finding| finding.code == "salvage.report_invariant_invalid"),
            "{inspected:?}"
        );
        assert_eq!(inspected.health, InspectionHealth::Clean);
    }

    #[test]
    fn salvage_copy_entry_and_byte_limits_are_exact() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        let source_before = tree_snapshot(&source);

        let measured =
            salvage_data_directory(&source, temp.path().join("measured"), salvage_limits())
                .unwrap();
        let exact = DataDirectorySalvageLimits {
            max_copy_entries: measured.copied_entries,
            max_copy_bytes: measured.copied_bytes,
            ..salvage_limits()
        };
        salvage_data_directory(&source, temp.path().join("exact"), exact).unwrap();

        let byte_short = DataDirectorySalvageLimits {
            max_copy_bytes: measured.copied_bytes - 1,
            ..exact
        };
        assert!(
            salvage_data_directory(&source, temp.path().join("byte-short"), byte_short).is_err()
        );
        let entry_short = DataDirectorySalvageLimits {
            max_copy_entries: measured.copied_entries - 1,
            ..exact
        };
        assert!(
            salvage_data_directory(&source, temp.path().join("entry-short"), entry_short).is_err()
        );

        assert_eq!(source_before, tree_snapshot(&source));
        assert!(!temp.path().join("byte-short").exists());
        assert!(!temp.path().join("entry-short").exists());
        assert!(salvage_staging_paths(temp.path()).is_empty());
    }

    #[test]
    fn salvage_report_item_and_path_ledgers_are_exact_with_many_wal_paths() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        for segment_id in 1..=8u64 {
            let relative = format!("wal/wal-{segment_id:016x}.log");
            std::fs::write(
                source.join(relative),
                wal_frame(
                    segment_id + 1,
                    &valid_series_definition_payload(&format!("later-{segment_id}")),
                ),
            )
            .unwrap();
        }
        let suffix = "x".repeat(96);
        let destination = |tag: char| temp.path().join(format!("ledger-{tag}-{suffix}"));
        let measured = salvage_data_directory(&source, destination('m'), salvage_limits()).unwrap();

        let exact_limits = DataDirectorySalvageLimits {
            inspection: DataDirectoryInspectionLimits {
                max_report_items: measured.retention.report_items,
                max_retained_path_bytes: measured.retention.retained_path_bytes,
                ..DataDirectoryInspectionLimits::default()
            },
            ..salvage_limits()
        };
        let exact = salvage_data_directory(&source, destination('e'), exact_limits).unwrap();
        assert_eq!(exact.retention, measured.retention);

        let item_short = DataDirectorySalvageLimits {
            inspection: DataDirectoryInspectionLimits {
                max_report_items: measured.retention.report_items - 1,
                ..exact_limits.inspection
            },
            ..exact_limits
        };
        assert!(salvage_data_directory(&source, destination('i'), item_short).is_err());

        let path_short = DataDirectorySalvageLimits {
            inspection: DataDirectoryInspectionLimits {
                max_retained_path_bytes: measured.retention.retained_path_bytes - 1,
                ..exact_limits.inspection
            },
            ..exact_limits
        };
        assert!(salvage_data_directory(&source, destination('p'), path_short).is_err());
    }

    #[test]
    fn salvage_refuses_existing_overlapping_and_raced_destinations() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        let source_before = tree_snapshot(&source);

        let existing = temp.path().join("existing");
        std::fs::write(&existing, b"do not replace").unwrap();
        assert!(salvage_data_directory(&source, &existing, salvage_limits()).is_err());
        assert_eq!(std::fs::read(&existing).unwrap(), b"do not replace");

        let overlapping = source.join("recovered");
        assert!(salvage_data_directory(&source, &overlapping, salvage_limits()).is_err());
        assert!(!overlapping.exists());

        let raced = temp.path().join("raced");
        let err =
            salvage_data_directory_with_before_publish(&source, &raced, salvage_limits(), |_| {
                std::fs::create_dir(&raced).unwrap();
                std::fs::write(raced.join("owner"), b"racer").unwrap();
                Ok(())
            })
            .unwrap_err();
        assert!(err.to_string().contains("exist") || err.to_string().contains("rename"));
        assert_eq!(std::fs::read(raced.join("owner")).unwrap(), b"racer");
        assert_eq!(source_before, tree_snapshot(&source));
        assert_eq!(salvage_staging_paths(temp.path()).len(), 1);
    }

    #[test]
    fn salvage_prepublication_failure_retains_owned_staging_for_inspection() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        let source_before = tree_snapshot(&source);

        let err = salvage_data_directory_with_before_publish(
            &source,
            &destination,
            salvage_limits(),
            |_| {
                Err(TsinkError::Other(
                    "injected prepublication failure".to_string(),
                ))
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("injected prepublication failure"));
        assert!(!destination.exists());
        assert_eq!(salvage_staging_paths(temp.path()).len(), 1);
        assert_eq!(source_before, tree_snapshot(&source));
    }

    #[test]
    fn salvage_static_source_symlink_is_refused_before_canonicalization() {
        #[cfg(unix)]
        {
            let temp = TempDir::new().unwrap();
            let real_source = temp.path().join("source-real");
            let source = temp.path().join("source-link");
            let destination = temp.path().join("recovered");
            std::fs::create_dir(&real_source).unwrap();
            create_current_corrupt_wal_source(&real_source, WalDamage::CorruptTail);
            std::os::unix::fs::symlink(&real_source, &source).unwrap();

            let err = salvage_data_directory(&source, &destination, salvage_limits()).unwrap_err();

            assert!(
                err.to_string().contains("symbolic")
                    || err.to_string().contains("not a directory")
                    || err.to_string().contains("IO error")
            );
            assert!(!destination.exists());
            assert!(salvage_staging_paths(temp.path()).is_empty());
        }
    }

    #[cfg(unix)]
    #[test]
    fn salvage_refuses_a_symlinked_destination_parent_ancestor() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&outside).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        std::os::unix::fs::symlink(&outside, temp.path().join("publish-link")).unwrap();

        let destination = temp.path().join("publish-link/recovered");
        assert!(salvage_data_directory(&source, &destination, salvage_limits()).is_err());
        assert!(!outside.join("recovered").exists());
        assert!(salvage_staging_paths(&outside).is_empty());
    }

    #[test]
    fn salvage_source_generation_change_is_detected_before_publication() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);

        let err = salvage_data_directory_with_before_publish(
            &source,
            &destination,
            salvage_limits(),
            |_| {
                std::fs::write(
                    source.join("wal/wal-0000000000000001.log"),
                    wal_frame(3, &valid_series_definition_payload("late-source-frame")),
                )
                .unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("source"));
        assert!(!destination.exists());
        assert_eq!(salvage_staging_paths(temp.path()).len(), 1);
    }

    #[test]
    fn salvage_staging_content_change_after_inspection_is_not_published() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);

        let err = salvage_data_directory_with_before_publish(
            &source,
            &destination,
            salvage_limits(),
            |staging| {
                let path = staging.join("series_index.bin");
                let mut bytes = std::fs::read(&path).unwrap();
                *bytes.last_mut().unwrap() ^= 1;
                std::fs::write(path, bytes).unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("staging"));
        assert!(!destination.exists());
        assert_eq!(salvage_staging_paths(temp.path()).len(), 1);
    }

    #[test]
    fn salvage_same_length_evidence_or_manifest_tampering_is_not_published() {
        for target in ["evidence", "manifest"] {
            let temp = TempDir::new().unwrap();
            let source = temp.path().join("source");
            let destination = temp.path().join("recovered");
            std::fs::create_dir(&source).unwrap();
            create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);

            let err = salvage_data_directory_with_before_publish(
                &source,
                &destination,
                salvage_limits(),
                |staging| {
                    let path = match target {
                        "evidence" => staging.join(SALVAGE_REPORT_FILE_NAME),
                        "manifest" => staging.join(DATA_DIRECTORY_MANIFEST_FILE_NAME),
                        _ => unreachable!(),
                    };
                    let mut bytes = std::fs::read(&path).unwrap();
                    let index = match target {
                        "evidence" => {
                            let marker = b"\"source_path\": \"";
                            let start = bytes
                                .windows(marker.len())
                                .position(|window| window == marker)
                                .unwrap()
                                + marker.len();
                            start
                        }
                        "manifest" => bytes.iter().position(|byte| *byte == b'\n').unwrap(),
                        _ => unreachable!(),
                    };
                    bytes[index] = match target {
                        "evidence" => b'.',
                        "manifest" => b' ',
                        _ => unreachable!(),
                    };
                    std::fs::write(path, bytes).unwrap();
                    Ok(())
                },
            )
            .unwrap_err();

            assert!(err.to_string().contains("bytes"), "{target}: {err}");
            assert!(!destination.exists());
            assert_eq!(salvage_staging_paths(temp.path()).len(), 1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn salvage_destination_parent_swap_cannot_redirect_publication() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let parent = temp.path().join("publish-parent");
        let displaced = temp.path().join("publish-parent-displaced");
        let outside = temp.path().join("outside");
        let destination = parent.join("recovered");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&parent).unwrap();
        std::fs::create_dir(&outside).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);

        let err = salvage_data_directory_with_before_publish(
            &source,
            &destination,
            salvage_limits(),
            |_| {
                std::fs::rename(&parent, &displaced).unwrap();
                std::os::unix::fs::symlink(&outside, &parent).unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("destination parent"));
        assert!(!outside.join("recovered").exists());
        assert!(!displaced.join("recovered").exists());
        assert_eq!(salvage_staging_paths(&displaced).len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn salvage_late_destination_parent_swap_is_reported_after_publication() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let parent = temp.path().join("publish-parent");
        let displaced = temp.path().join("publish-parent-displaced");
        let outside = temp.path().join("outside");
        let destination = parent.join("recovered");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&parent).unwrap();
        std::fs::create_dir(&outside).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);

        let err = salvage_data_directory_with_publish_hooks(
            &source,
            &destination,
            salvage_limits(),
            |_| Ok(()),
            |_| {
                std::fs::rename(&parent, &displaced).unwrap();
                std::os::unix::fs::symlink(&outside, &parent).unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("became published")
                && err
                    .to_string()
                    .contains("requested path no longer resolves"),
            "{err}"
        );
        assert!(!outside.join("recovered").exists());
        assert!(displaced.join("recovered").is_dir());
        assert!(salvage_staging_paths(&displaced).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn salvage_late_staging_content_change_is_reported_after_publication() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);

        let err = salvage_data_directory_with_publish_hooks(
            &source,
            &destination,
            salvage_limits(),
            |_| Ok(()),
            |staging| {
                let manifest = staging.join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
                let mut bytes = std::fs::read(&manifest).unwrap();
                let byte = bytes
                    .iter_mut()
                    .find(|byte| **byte != b'\n')
                    .expect("manifest has a mutable non-newline byte");
                *byte ^= 1;
                std::fs::write(manifest, bytes).unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("became published")
                && (err.to_string().contains("namespace generation changed")
                    || err.to_string().contains("bytes differ")),
            "{err}"
        );
        assert!(destination.is_dir());
        assert!(salvage_staging_paths(temp.path()).is_empty());
    }

    #[test]
    fn persisted_salvage_report_requires_exact_unique_evidence_sets() {
        for case in [
            "zero_reset",
            "arbitrary_range",
            "duplicate_range",
            "missing_omitted",
            "duplicate_omitted",
            "duplicate_disposition",
        ] {
            let temp = TempDir::new().unwrap();
            let source = temp.path().join("source");
            let destination = temp.path().join("recovered");
            std::fs::create_dir(&source).unwrap();
            create_current_corrupt_wal_source(&source, WalDamage::MidLogChecksum);
            let later_relative = PathBuf::from("wal/wal-0000000000000001.log");
            std::fs::write(
                source.join(&later_relative),
                wal_frame(2, &valid_series_definition_payload("later_valid_segment")),
            )
            .unwrap();
            write_published(&source, 1, 3);
            salvage_data_directory(&source, &destination, salvage_limits()).unwrap();

            let report_path = destination.join(SALVAGE_REPORT_FILE_NAME);
            let mut persisted: PersistedSalvageReport =
                serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
            match case {
                "zero_reset" => {
                    let range = persisted
                        .discarded_ranges
                        .iter_mut()
                        .find(|range| {
                            range.reason_code == "wal.salvage_v1_full_reset"
                                && range.bytes.end > range.bytes.start
                        })
                        .unwrap();
                    range.bytes.end = range.bytes.start;
                }
                "arbitrary_range" => {
                    let mut range = persisted.discarded_ranges[0].clone();
                    range.path = "wal/not-a-canonical-segment.log".to_string();
                    range.reason_code = "wal.fabricated_reason".to_string();
                    persisted.discarded_ranges.push(range);
                }
                "duplicate_range" => {
                    let duplicate = persisted.discarded_ranges[0].clone();
                    persisted.discarded_ranges.push(duplicate);
                }
                "missing_omitted" => persisted.omitted_wal_paths.clear(),
                "duplicate_omitted" => {
                    let duplicate = persisted.omitted_wal_paths[0].clone();
                    persisted.omitted_wal_paths.push(duplicate);
                }
                "duplicate_disposition" => {
                    let duplicate = persisted.path_dispositions[0].clone();
                    persisted.path_dispositions.push(duplicate);
                }
                _ => unreachable!(),
            }
            std::fs::write(&report_path, serde_json::to_vec_pretty(&persisted).unwrap()).unwrap();

            let inspected =
                inspect_data_directory(&destination, DataDirectoryInspectionLimits::default())
                    .unwrap();
            assert!(
                inspected
                    .findings
                    .iter()
                    .any(|finding| finding.code == "salvage.report_invariant_invalid"),
                "{case}: {inspected:?}"
            );
        }
    }

    #[test]
    fn salvage_failure_retains_late_injected_descendant_without_recursive_cleanup() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        let measured =
            salvage_data_directory(&source, temp.path().join("measured"), salvage_limits())
                .unwrap();
        let limits = DataDirectorySalvageLimits {
            max_copy_entries: measured.copied_entries,
            ..salvage_limits()
        };

        let err =
            salvage_data_directory_with_before_publish(&source, &destination, limits, |staging| {
                std::fs::write(staging.join("late-owner-entry"), b"do not discover").unwrap();
                Err(TsinkError::Other(
                    "injected failure after late entry".to_string(),
                ))
            })
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("injected failure after late entry"));
        let staging = salvage_staging_paths(temp.path());
        assert_eq!(staging.len(), 1);
        assert_eq!(
            std::fs::read(staging[0].join("late-owner-entry")).unwrap(),
            b"do not discover"
        );
        assert!(!destination.exists());
    }

    #[test]
    fn salvage_refuses_non_wal_damage_and_incomplete_inspection() {
        for case in [
            "foreign",
            "manifest",
            "segment",
            "catalog",
            "registry",
            "registry_missing",
            "registry_delta",
            "post_flush",
            "tombstones",
            "wal_tmp",
        ] {
            let temp = TempDir::new().unwrap();
            let source = temp.path().join("source");
            let destination = temp.path().join("recovered");
            std::fs::create_dir(&source).unwrap();
            create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
            match case {
                "foreign" => {
                    std::fs::write(source.join("foreign-owner-file"), b"x").unwrap();
                }
                "manifest" => {
                    let path = source.join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
                    let mut value: serde_json::Value =
                        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    value["payload_crc32"] = serde_json::json!(0);
                    std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
                }
                "segment" => {
                    std::fs::create_dir_all(
                        source.join("lane_numeric/segments/L0/seg-0000000000000001"),
                    )
                    .unwrap();
                }
                "catalog" => {
                    std::fs::write(source.join("series_index.catalog.json"), b"{").unwrap();
                }
                "registry" => {
                    std::fs::write(source.join("series_index.bin"), b"corrupt registry").unwrap();
                }
                "registry_missing" => {
                    std::fs::remove_file(source.join("series_index.bin")).unwrap();
                }
                "registry_delta" => {
                    std::fs::write(source.join("series_index.delta.bin"), b"corrupt").unwrap();
                }
                "post_flush" => {
                    let recovery = source.join(".post-flush-replacements");
                    std::fs::create_dir(&recovery).unwrap();
                    std::fs::write(recovery.join("marker"), b"corrupt").unwrap();
                }
                "tombstones" => {
                    std::fs::create_dir_all(source.join("lane_numeric")).unwrap();
                    std::fs::write(source.join("lane_numeric/tombstones.json"), b"{").unwrap();
                }
                "wal_tmp" => {
                    std::fs::write(source.join("wal/wal.published.tmp"), b"stale").unwrap();
                }
                _ => unreachable!(),
            }
            let inspection =
                inspect_data_directory(&source, DataDirectoryInspectionLimits::default()).unwrap();
            if case == "registry" {
                assert!(inspection
                    .findings
                    .iter()
                    .any(|finding| finding.code == "registry.snapshot_corrupt"));
            }
            if case == "registry_missing" {
                assert!(inspection
                    .findings
                    .iter()
                    .any(|finding| finding.code == "registry.snapshot_missing"));
            }
            if matches!(
                case,
                "registry_delta" | "post_flush" | "tombstones" | "wal_tmp"
            ) {
                assert!(inspection
                    .findings
                    .iter()
                    .any(|finding| finding.code == "namespace.recovery_object_unverified"));
            }
            let source_before = tree_snapshot(&source);
            assert!(
                salvage_data_directory(&source, &destination, salvage_limits()).is_err(),
                "{case} must be refused"
            );
            assert!(!destination.exists());
            assert_eq!(source_before, tree_snapshot(&source));
            assert!(salvage_staging_paths(temp.path()).is_empty());
        }

        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("recovered");
        std::fs::create_dir(&source).unwrap();
        create_current_corrupt_wal_source(&source, WalDamage::CorruptTail);
        let limits = DataDirectorySalvageLimits {
            inspection: DataDirectoryInspectionLimits {
                max_wal_frames: 1,
                ..DataDirectoryInspectionLimits::default()
            },
            ..salvage_limits()
        };
        assert!(salvage_data_directory(&source, &destination, limits).is_err());
        assert!(!destination.exists());
        assert!(salvage_staging_paths(temp.path()).is_empty());
    }
}
